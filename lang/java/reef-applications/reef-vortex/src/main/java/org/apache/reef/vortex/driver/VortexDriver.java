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
package org.apache.reef.vortex.driver;

import org.apache.commons.lang.SerializationUtils;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.*;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.driver.task.TaskMessage;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.vortex.api.VortexStart;
import org.apache.reef.vortex.common.CacheDataRequest;
import org.apache.reef.vortex.common.TaskletFailureReport;
import org.apache.reef.vortex.common.TaskletResultReport;
import org.apache.reef.vortex.common.WorkerReport;
import org.apache.reef.vortex.common.exceptions.VortexCacheException;
import org.apache.reef.vortex.evaluator.VortexWorker;
import org.apache.reef.vortex.failure.VortexPoisonedContextStartHandler;
import org.apache.reef.vortex.failure.parameters.IntervalMs;
import org.apache.reef.vortex.failure.parameters.Probability;
import org.apache.reef.vortex.trace.parameters.ReceiverHost;
import org.apache.reef.vortex.trace.parameters.ReceiverPort;
import org.apache.reef.vortex.trace.parameters.ReceiverType;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.SingleThreadStage;
import org.apache.reef.wake.impl.ThreadPoolStage;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * REEF Driver for Vortex.
 */
@Unit
@DriverSide
final class VortexDriver {
  private static final Logger LOG = Logger.getLogger(VortexDriver.class.getName());
  private static final int MAX_NUM_OF_FAILURES = 1600;
  private static final int SCHEDULER_EVENT = 0; // Dummy number to comply with onNext() interface

  private final AtomicInteger numberOfFailures = new AtomicInteger(0);
  private final EvaluatorRequestor evaluatorRequestor; // for requesting resources
  private final VortexMaster vortexMaster; // Vortex Master
  private final VortexRequestor vortexRequestor; // For sending Commands to remote workers

  // Resource configuration for single thread pool
  private final int evalMem;
  private final int evalNum;
  private final int evalCores;
  private final int workerCapacity;
  private final String receiverType;
  private final String receiverHost;
  private final int receiverPort;


  private final EStage<VortexStart> vortexStartEStage;
  private final VortexStart vortexStart;
  private final EStage<Integer> pendingTaskletSchedulerEStage;

  private final AtomicInteger barrier;
  private final double failureProbability;
  private final int failureInterval;

  @Inject
  private VortexDriver(final EvaluatorRequestor evaluatorRequestor,
                       final VortexRequestor vortexRequestor,
                       final VortexMaster vortexMaster,
                       final VortexStart vortexStart,
                       final VortexStartExecutor vortexStartExecutor,
                       final PendingTaskletLauncher pendingTaskletLauncher,
                       @Parameter(VortexMasterConf.WorkerMem.class) final int workerMem,
                       @Parameter(VortexMasterConf.WorkerNum.class) final int workerNum,
                       @Parameter(VortexMasterConf.WorkerCores.class) final int workerCores,
                       @Parameter(VortexMasterConf.WorkerCapacity.class) final int workerCapacity,
                       @Parameter(VortexMasterConf.NumberOfVortexStartThreads.class) final int numOfStartThreads,
                       @Parameter(ReceiverType.class) final String receiverType,
                       @Parameter(ReceiverHost.class) final String receiverHost,
                       @Parameter(ReceiverPort.class) final int receiverPort,
                       @Parameter(Probability.class) final double failureProbability,
                       @Parameter(IntervalMs.class) final int failureInterval) {
    this.vortexStartEStage = new ThreadPoolStage<>(vortexStartExecutor, numOfStartThreads);
    this.vortexStart = vortexStart;
    this.pendingTaskletSchedulerEStage = new SingleThreadStage<>(pendingTaskletLauncher, 1);
    this.evaluatorRequestor = evaluatorRequestor;
    this.vortexMaster = vortexMaster;
    this.vortexRequestor = vortexRequestor;
    this.evalMem = workerMem;
    this.evalNum = workerNum;
    this.evalCores = workerCores;
    this.workerCapacity = workerCapacity;
    this.barrier = new AtomicInteger(workerNum);
    this.receiverType = receiverType;
    this.receiverHost = receiverHost;
    this.receiverPort = receiverPort;
    this.failureProbability = failureProbability;
    this.failureInterval = failureInterval;
  }

  /**
   * Driver started.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      // Initial Evaluator Request
      evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(evalNum)
          .setMemory(evalMem)
          .setNumberOfCores(evalCores)
          .build());
    }
  }

  /**
   * Container allocated.
   */
  final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Container allocated");
      final String workerId = allocatedEvaluator.getId() + "_vortex_worker";

      final Configuration workerConfiguration = VortexWorkerConf.CONF
          .set(VortexWorkerConf.RECEIVER_TYPE, receiverType)
          .set(VortexWorkerConf.RECEIVER_HOST, receiverHost)
          .set(VortexWorkerConf.RECEIVER_PORT, receiverPort)
          .set(VortexWorkerConf.NUM_OF_THREADS, workerCapacity) // NUM_OF_THREADS = evalCores
          .build();

      final Configuration contextConfiguration =
          Configurations.merge(
              Tang.Factory.getTang().newConfigurationBuilder()
                  .bindNamedParameter(Probability.class, Double.toString(failureProbability))
                  .bindNamedParameter(IntervalMs.class, Integer.toString(failureInterval))
                  .build(),
              ContextConfiguration.CONF
                  .set(ContextConfiguration.IDENTIFIER, "vortex_worker")
                  .set(ContextConfiguration.ON_CONTEXT_STARTED, VortexPoisonedContextStartHandler.class)
                  .build());

      final Configuration taskConfiguration = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, workerId)
          .set(TaskConfiguration.TASK, VortexWorker.class)
          .set(TaskConfiguration.ON_SEND_MESSAGE, VortexWorker.class)
          .set(TaskConfiguration.ON_MESSAGE, VortexWorker.DriverMessageHandler.class)
          .set(TaskConfiguration.ON_CLOSE, VortexWorker.TaskCloseHandler.class)
          .build();

      allocatedEvaluator.submitContextAndTask(contextConfiguration,
          Configurations.merge(workerConfiguration, taskConfiguration));
    }
  }

  /**
   * Evaluator up and running.
   */
  final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask reefTask) {
      LOG.log(Level.INFO, "Worker up and running");
      vortexMaster.workerAllocated(new VortexWorkerManager(vortexRequestor, reefTask));

      final int num = barrier.decrementAndGet();
      if (num == 0) {
        // Run Vortex Start
        vortexStartEStage.onNext(vortexStart);

        // Run Scheduler
        pendingTaskletSchedulerEStage.onNext(SCHEDULER_EVENT);
      }
      LOG.log(Level.INFO, "NUM: " + num);
    }
  }

  /**
   * Message received.
   */
  final class TaskMessageHandler implements EventHandler<TaskMessage> {
    @Override
    public void onNext(final TaskMessage taskMessage) {
      final String workerId = taskMessage.getId();
      final WorkerReport workerReport= (WorkerReport)SerializationUtils.deserialize(taskMessage.get());
      switch (workerReport.getType()) {
      case TaskletResult:
        final TaskletResultReport taskletResultReport = (TaskletResultReport)workerReport;
        vortexMaster.taskletCompleted(workerId, taskletResultReport.getTaskletId(), taskletResultReport.getResult());
        break;
      case TaskletFailure:
        final TaskletFailureReport taskletFailureReport = (TaskletFailureReport)workerReport;
        vortexMaster.taskletErrored(workerId, taskletFailureReport.getTaskletId(), taskletFailureReport.getException());
        break;
      case CacheRequest:
        final CacheDataRequest cacheDataRequest = (CacheDataRequest)workerReport;
        try (final TraceScope dataRequestScope =
                 Trace.startSpan("master_data_requested", cacheDataRequest.getTraceInfo())){
          vortexMaster.dataRequested(workerId, cacheDataRequest.getCacheKey(), dataRequestScope.getSpan());
        } catch (VortexCacheException e) {
          LOG.log(Level.SEVERE, "Failed to load the data that worker {0} requested with key name {1}.",
              new Object[] {workerId, cacheDataRequest.getCacheKey().getName()});
        }
        break;
      default:
        throw new RuntimeException("Unknown Report");
      }
    }
  }

  /**
   * Evaluator preempted.
   * TODO[REEF-501]: Distinguish different types of FailedEvaluator in Vortex.
   */
  final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      LOG.log(Level.INFO, "Evaluator preempted");
      if (numberOfFailures.incrementAndGet() >= MAX_NUM_OF_FAILURES) {
        throw new RuntimeException("Exceeded max number of failures");
      } else {
        // We request a new evaluator to take the place of the preempted one
        evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
            .setNumber(1)
            .setMemory(evalMem)
            .setNumberOfCores(evalCores)
            .build());

        if (failedEvaluator.getFailedTask().isPresent()) {
          vortexMaster.workerPreempted(failedEvaluator.getFailedTask().get().getId());
        } else {
          LOG.log(Level.WARNING, "Worker preempted, but not recoverable.");
        }
      }
    }
  }
}
