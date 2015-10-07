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
import org.apache.htrace.Trace;
import org.apache.htrace.TraceInfo;
import org.apache.htrace.TraceScope;
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
import org.apache.reef.vortex.trace.HTrace;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.*;

/**
 * Receives commands from VortexMaster, executes them, and returns the results.
 * TODO[REEF-503]: Basic Vortex profiling.
 */
@Unstable
@Unit
@TaskSide
public final class VortexWorker implements Task, TaskMessageSource {
  private static final String TASKLET_EXECUTE_SPAN = "worker_execute";
  private static final String RESULT_SERIALIZE_SPAN = "worker_serialize";
  private static final String RECEIVE_CACHE_SPAN = "worker_receive_cache";
  private static final String MESSAGE_SOURCE_ID = ""; // empty string as there is no use for it

  private final BlockingDeque<byte[]> pendingRequests = new LinkedBlockingDeque<>();
  private final BlockingDeque<byte[]> workerReports = new LinkedBlockingDeque<>();

  private final HeartBeatTriggerManager heartBeatTriggerManager;
  private final VortexCache cache;
  private final int numOfThreads;
  private final int numOfSlackThreads;
  private final CountDownLatch terminated = new CountDownLatch(1);

  @Inject
  private VortexWorker(final HeartBeatTriggerManager heartBeatTriggerManager,
                       final VortexCache cache,
                       @Parameter(VortexWorkerConf.NumOfThreads.class) final int numOfThreads,
                       @Parameter(VortexWorkerConf.NumOfSlackThreads.class) final int numOfSlackThreads,
                       final HTrace hTrace) {
    hTrace.initialize();
    this.heartBeatTriggerManager = heartBeatTriggerManager;
    this.cache = cache;
    this.numOfThreads = numOfThreads;
    this.numOfSlackThreads = numOfSlackThreads;
  }

  /**
   * Starts the scheduler & executor and waits until termination.
   */
  @Override
  public byte[] call(final byte[] memento) throws Exception {
    final ExecutorService schedulerThread = Executors.newSingleThreadExecutor();
    final ExecutorService commandExecutor = Executors.newFixedThreadPool(numOfThreads + numOfSlackThreads);

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

          final long traceId = ByteBuffer.wrap(Arrays.copyOfRange(message, 0, Long.SIZE / Byte.SIZE)).getLong();
          final long spanId =
              ByteBuffer.wrap(Arrays.copyOfRange(message, Long.SIZE / Byte.SIZE, 2 * (Long.SIZE / Byte.SIZE)))
                  .getLong();
          final TraceInfo traceInfo = new TraceInfo(traceId, spanId);


          // Scheduler Thread: Pass the command to the worker thread pool to be executed
          commandExecutor.execute(new Runnable() {
            @Override
            public void run() {
              // Command Executor: Deserialize the command

              final VortexRequest vortexRequest;

              final byte[] request;
              try (final TraceScope traceScope =
                       Trace.startSpan("worker_arrayCopy", traceInfo)) {
                request = Arrays.copyOfRange(message, 2 * (Long.SIZE / Byte.SIZE), message.length);
              }


              try (final TraceScope traceScope =
                       Trace.startSpan("worker_deserialize " + request.length/1024/1024.0 + "mb", traceInfo)) {
                vortexRequest = (VortexRequest) SerializationUtils.deserialize(request);
              }


              switch (vortexRequest.getType()) {
                case ExecuteTasklet:
                  final TaskletExecutionRequest taskletExecutionRequest = (TaskletExecutionRequest) vortexRequest;
                  try {
                    // Command Executor: Execute the command
                    final Serializable result;
                    try (final TraceScope traceScope =
                             Trace.startSpan(TASKLET_EXECUTE_SPAN, traceInfo)) {
                      result = taskletExecutionRequest.execute();
                    }

                    // Command Executor: Tasklet successfully returns result
                    final WorkerReport report =
                        new TaskletResultReport<>(taskletExecutionRequest.getTaskletId(), result);
                    final byte[] reportBytes;
                    try (final TraceScope traceScope =
                             Trace.startSpan(RESULT_SERIALIZE_SPAN, traceInfo)) {
                      reportBytes = SerializationUtils.serialize(report);
                    }
                    workerReports.addLast(reportBytes);
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
                  try (final TraceScope traceScope =
                             Trace.startSpan(RECEIVE_CACHE_SPAN, traceInfo)) {
                    cache.notifyOnArrival(cacheSentRequest.getCacheKey(), cacheSentRequest.getData());
                  }
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
  void sendDataRequest(final CacheKey key) throws InterruptedException {
    final WorkerReport report = new CacheDataRequest(key);
    workerReports.addLast(SerializationUtils.serialize(report));
    heartBeatTriggerManager.triggerHeartBeat();
  }

  /**
   * Handle requests from Vortex Master.
   */
  public final class DriverMessageHandler implements EventHandler<DriverMessage> {
    @Override
    public void onNext(final DriverMessage message) {
      if (message.get().isPresent()) {
        final byte[] bytes = message.get().get();
        final long traceId = ByteBuffer.wrap(Arrays.copyOfRange(bytes, 0, Long.SIZE / Byte.SIZE)).getLong();
        final long spanId =
            ByteBuffer.wrap(Arrays.copyOfRange(bytes, Long.SIZE / Byte.SIZE, 2 * (Long.SIZE / Byte.SIZE))).getLong();
        try (final TraceScope traceScope =
                 Trace.startSpan("Worker_Enqueued", new TraceInfo(traceId, spanId))) {
          pendingRequests.addLast(message.get().get());
        }
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
