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
package org.apache.reef.examples.prod;

import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver code for production job.
 */
@Unit
public final class ProductionDriver {
  private static final Logger LOG = Logger.getLogger(ProductionDriver.class.getName());

  private final EvaluatorRequestor evaluatorRequestor;
  private final int numTasks;
  private final int delay;
  private final AtomicInteger numSubmittedTasks = new AtomicInteger(0);
  private final Set<AllocatedEvaluator> evaluators =
      Collections.newSetFromMap(new ConcurrentHashMap<AllocatedEvaluator, Boolean>());

  @Inject
  private ProductionDriver(final EvaluatorRequestor evaluatorRequestor,
                           @Parameter(ProductionREEF.NumTasks.class) final Integer numTasks,
                           @Parameter(ProductionREEF.Delay.class) final Integer delay) {
    this.evaluatorRequestor = evaluatorRequestor;
    this.numTasks = numTasks;
    this.delay = delay;
  }

  /**
   * Job Driver is ready and the clock is set up: request the evaluators.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "TIME: Start Driver with {0} Evaluators", numTasks);
      evaluatorRequestor.submit(
          EvaluatorRequest.newBuilder()
              .setMemory(6144)
              .setNumberOfCores(8)
              .setNumber(numTasks).build()
      );
    }
  }

  /**
   * Job Driver is is shutting down: write to the log.
   */
  final class StopHandler implements EventHandler<StopTime> {
    @Override
    public void onNext(final StopTime stopTime) {
      LOG.log(Level.INFO, "TIME: Stop Driver");
    }
  }

  /**
   * When Evaluators are allocated, submit tasks.
   */
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator evaluator) {
      final Configuration taskConf =
          Tang.Factory.getTang().newConfigurationBuilder(
              TaskConfiguration.CONF
                  .set(TaskConfiguration.IDENTIFIER, "task" + numSubmittedTasks.incrementAndGet())
                  .set(TaskConfiguration.TASK, SleepTask.class)
                  .build())
          .bindNamedParameter(ProductionREEF.Delay.class, String.valueOf(delay))
          .build();
      evaluators.add(evaluator);
      LOG.log(Level.INFO, "{0} evaluators are allocated", evaluators.size());
      if (evaluators.size() == numTasks) {
        LOG.log(Level.INFO, "All evaluators are ready. Submit Tasks");
        for (final AllocatedEvaluator e : evaluators) {
          e.submitTask(taskConf);
        }
      }
    }
  }
}
