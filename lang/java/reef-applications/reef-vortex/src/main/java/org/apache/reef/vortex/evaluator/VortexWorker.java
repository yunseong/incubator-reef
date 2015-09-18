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
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.task.HeartBeatTriggerManager;
import org.apache.reef.task.Task;
import org.apache.reef.task.TaskMessage;
import org.apache.reef.task.TaskMessageSource;
import org.apache.reef.task.events.CloseEvent;
import org.apache.reef.task.events.DriverMessage;
import org.apache.reef.util.Optional;
import org.apache.reef.vortex.common.*;
import org.apache.reef.vortex.common.exceptions.VortexCacheException;
import org.apache.reef.vortex.driver.VortexWorkerConf;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.concurrent.*;

/**
 * Receives commands from VortexMaster, executes them, and returns the results.
 * TODO[REEF-503]: Basic Vortex profiling.
 */
@Unstable
@Unit
@TaskSide
public final class VortexWorker implements Task, TaskMessageSource {
  private static final String MESSAGE_SOURCE_ID = ""; // empty string as there is no use for it

  private final BlockingDeque<byte[]> pendingRequests = new LinkedBlockingDeque<>();
  private final BlockingDeque<byte[]> workerReports = new LinkedBlockingDeque<>();

  private final HeartBeatTriggerManager heartBeatTriggerManager;
  private final VortexCache vortexCache;
  private final int numOfThreads;
  private final CountDownLatch terminated = new CountDownLatch(1);

  @Inject
  private VortexWorker(final HeartBeatTriggerManager heartBeatTriggerManager,
                       final VortexCache cache,
                      @Parameter(VortexWorkerConf.NumOfThreads.class) final int numOfThreads) {
    this.heartBeatTriggerManager = heartBeatTriggerManager;
    this.vortexCache = cache;
    this.numOfThreads = numOfThreads;
  }

  /**
   * Starts the scheduler & executor and waits until termination.
   */
  @Override
  public byte[] call(final byte[] memento) throws Exception {
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
                case CacheSent:
                  final CacheSentRequest cacheSentRequest = (CacheSentRequest) vortexRequest;
                  vortexCache.onDataArrived(cacheSentRequest.getCacheKey(), cacheSentRequest.getData());
                  break;
                default:
                  throw new RuntimeException("Unknown Command");
              }
            }
          });

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
   * Send the request for the cached data to Master.
   * @param key Key of the data.
   */
  void sendDataRequest(final CacheKey key) {
    final WorkerReport report = new CacheDataRequest(key);
    workerReports.addLast(SerializationUtils.serialize(report));
    heartBeatTriggerManager.triggerHeartBeat();
  }

  /**
   * Retrieve the data from the cache. If the data has not been loaded yet,
   * the thread will be blocked until the data is fetched from the Master.
   * @param key Key of the data.
   * @param <T> Type of the data.
   * @return Data loaded from the cache.
   * @throws VortexCacheException If it failed to load the data.
   */
  public static <T extends Serializable> T getCachedData(final CacheKey<T> key) throws VortexCacheException {
    try {
      final VortexCache cache = Tang.Factory.getTang().newInjector().getInstance(VortexCache.class);
      return cache.get(key);
    } catch (InjectionException e) {
      throw new VortexCacheException("Failed to retrieve the data from the cache");
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
