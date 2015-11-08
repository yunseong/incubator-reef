/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.vortex.evaluator;

import org.apache.commons.lang.SerializationUtils;
import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.task.HeartBeatTriggerManager;
import org.apache.reef.task.Task;
import org.apache.reef.task.TaskMessage;
import org.apache.reef.task.TaskMessageSource;
import org.apache.reef.task.events.CloseEvent;
import org.apache.reef.task.events.DriverMessage;
import org.apache.reef.util.Optional;
import org.apache.reef.vortex.common.*;
import org.apache.reef.vortex.driver.VortexWorkerConf;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Receives commands from VortexMaster, executes them, and returns the results.
 * TODO[REEF-503]: Basic Vortex profiling.
 */
@Unstable
@Unit
@TaskSide
public final class VortexWorker implements Task, TaskMessageSource {
  private static final Logger LOG = Logger.getLogger(VortexWorker.class.getName());
  private static final String MESSAGE_SOURCE_ID = ""; // empty string as there is no use for it
  private static final int STRAGGLER_THRESHOLD_MILLIS = 3000;
  private static final int STRAGGLER_MONITOR_PREIOD = 2000;

  private final BlockingDeque<byte[]> pendingRequests = new LinkedBlockingDeque<>();
  private final BlockingDeque<byte[]> workerReports = new LinkedBlockingDeque<>();

  private final HeartBeatTriggerManager heartBeatTriggerManager;
  private final int numOfThreads;
  private final CountDownLatch terminated = new CountDownLatch(1);
  private final ConcurrentMap<Integer, Long> taskletIdToStartTimeMap = new ConcurrentHashMap<>();

  @Inject
  private VortexWorker(final HeartBeatTriggerManager heartBeatTriggerManager,
                       @Parameter(VortexWorkerConf.NumOfThreads.class) final int numOfThreads) {
    this.heartBeatTriggerManager = heartBeatTriggerManager;
    this.numOfThreads = numOfThreads;
  }

  /**
   * Starts the scheduler & executor and waits until termination.
   */
  @Override
  public byte[] call(final byte[] memento) throws Exception {

    final ExecutorService stragglerMonitorThread = Executors.newSingleThreadExecutor();
    final ExecutorService schedulerThread = Executors.newSingleThreadExecutor();
    final ExecutorService commandExecutor = Executors.newFixedThreadPool(numOfThreads);

    // Scheduling thread starts
    schedulerThread.execute(new Runnable() {
      @Override
      public void run() {
        while (true) {
          // Scheduler Thread: Pick a command to execute (For now, simple FIFO order)
          final byte[] message;
          try {
            message = pendingRequests.takeFirst();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }

          // Scheduler Thread: Pass the command to the worker thread pool to be executed
          commandExecutor.execute(new Runnable() {
            @Override
            public void run() {
              // Command Executor: Deserialize the command
              final VortexRequest vortexRequest = (VortexRequest) SerializationUtils.deserialize(message);
              switch (vortexRequest.getType()) {
                case ExecuteTasklet:
                  final TaskletExecutionRequest taskletExecutionRequest = (TaskletExecutionRequest) vortexRequest;
                  try {
                    // Command Executor: Execute the command
                    taskletIdToStartTimeMap.putIfAbsent(taskletExecutionRequest.getTaskletId(),
                        System.currentTimeMillis());
                    final Serializable result = taskletExecutionRequest.execute();

                    // Command Executor: Tasklet successfully returns result
                    final WorkerReport report =
                        new TaskletResultReport<>(taskletExecutionRequest.getTaskletId(), result);
                    workerReports.addLast(SerializationUtils.serialize(report));
                  } catch (Exception e) {
                    // Command Executor: Tasklet throws an exception
                    final WorkerReport report =
                        new TaskletFailureReport(taskletExecutionRequest.getTaskletId(), e);
                    workerReports.addLast(SerializationUtils.serialize(report));
                  }

                  heartBeatTriggerManager.triggerHeartBeat();
                  break;
                default:
                  throw new RuntimeException("Unknown Command");
              }
            }
          });

        }
      }
    });

    stragglerMonitorThread.execute(new Runnable() {
      @Override
      public void run() {
        try {
          while (true) {
            // Look up progress tracker, and report if there are Tasklets which take longer than threshold.
            Thread.sleep(STRAGGLER_MONITOR_PREIOD);

            final long currentTime = System.currentTimeMillis();
            for (final Map.Entry<Integer, Long> entry : taskletIdToStartTimeMap.entrySet()) {
              final int taskletId = entry.getKey();
              final long startTime = entry.getValue();
              if (currentTime - startTime > STRAGGLER_THRESHOLD_MILLIS) {
                LOG.log(Level.INFO, "Tasklet {0} seems to be a straggler", taskletId);
                final WorkerReport report = new TaskletStragglerReport(taskletId);
                workerReports.addLast(SerializationUtils.serialize(report));
              }
            }
            heartBeatTriggerManager.triggerHeartBeat();
          }
        } catch (final InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    });

    terminated.await();
    return null;
  }

  /**
   * @return the workerReport the worker wishes to send.
   */
  @Override
  public Optional<TaskMessage> getMessage() {
    final byte[] msg = workerReports.pollFirst();
    if (msg != null) {
      return Optional.of(TaskMessage.from(MESSAGE_SOURCE_ID, msg));
    } else {
      return Optional.empty();
    }
  }

  /**
   * Handle requests from Vortex Master.
   */
  public final class DriverMessageHandler implements EventHandler<DriverMessage> {
    @Override
    public void onNext(final DriverMessage message) {
      if (message.get().isPresent()) {
        pendingRequests.addLast(message.get().get());
      }
    }
  }

  /**
   * Shut down this worker.
   */
  public final class TaskCloseHandler implements EventHandler<CloseEvent> {
    @Override
    public void onNext(final CloseEvent closeEvent) {
      terminated.countDown();
    }
  }
}
