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

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.util.Optional;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * Test SchedulingPolicy.
 */
public class SchedulingPolicyTest {
  private final TestUtil testUtil = new TestUtil();

  /**
   * Test common traits of different scheduling policies.
   */
  @Test
  public void testCommon() throws Exception {
    commonPolicyTests(new RandomSchedulingPolicy());
    commonPolicyTests(new FirstFitSchedulingPolicy(10));
  }

  @Test
  public void testStragglerHandlingNoStraggler() throws Exception {
    final int workerCapacity = 1;
    final int numOfWorkers = 5;

    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(VortexMasterConf.WorkerCapacity.class, Integer.toString(workerCapacity)).build();
    final StragglerHandlingSchedulingPolicy policy =
        Tang.Factory.getTang().newInjector(conf).getInstance(StragglerHandlingSchedulingPolicy.class);

    // Add workers
    final Deque<VortexWorkerManager> workers = new ArrayDeque<>();
    for (int i = 0; i < numOfWorkers; i++) {
      final VortexWorkerManager worker = testUtil.newWorker();
      workers.addFirst(worker);
      policy.workerAdded(worker);
    }

    final Tasklet tasklet = testUtil.newTasklet();
    final Optional<String> workerId = policy.trySchedule(tasklet);
    assertTrue("Should assign one worker", workerId.isPresent());

    final VortexWorkerManager worker = workers.getFirst();
    policy.taskletLaunched(worker, tasklet);

    // Terminate right away
    policy.taskletCompleted(worker, tasklet);

    assertFalse("Should be empty", policy.getScheduled().containsKey(tasklet.getId()));
  }


  @Test
  public void testStragglerHandlingWithStraggler() throws Exception {
    final int workerCapacity = 1;
    final int numOfWorkers = 5;
    final int maxDuplicate = 3;
    final int threshold = 1000;
    final int checkingPeriod = 500;

    final PendingTasklets pendingTasklets = Tang.Factory.getTang().newInjector().getInstance(PendingTasklets.class);
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(VortexMasterConf.WorkerCapacity.class, Integer.toString(workerCapacity))
        .bindNamedParameter(VortexMasterConf.MaxDuplicates.class, Integer.toString(maxDuplicate))
        .bindNamedParameter(VortexMasterConf.StragglerThresholdMillis.class, Integer.toString(threshold))
        .bindNamedParameter(VortexMasterConf.StragglerCheckingPeriodMillis.class, Integer.toString(checkingPeriod))
        .build();

    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    injector.bindVolatileInstance(PendingTasklets.class, pendingTasklets);
    final StragglerHandlingSchedulingPolicy policy = injector.getInstance(StragglerHandlingSchedulingPolicy.class);

    // Add workers
    final Map<String, VortexWorkerManager> workers = new HashMap<>();
    for (int i = 0; i < numOfWorkers; i++) {
      final VortexWorkerManager worker = testUtil.newWorker();
      workers.put(worker.getId(), worker);
      policy.workerAdded(worker);
    }

    final Set<String> launchedWorkers = new HashSet<>();

    final Tasklet tasklet = testUtil.newTasklet();
    final Optional<String> firstWorkerId = policy.trySchedule(tasklet);
    assertTrue("Should assign one worker", firstWorkerId.isPresent());

    final VortexWorkerManager firstWorker = workers.get(firstWorkerId.get());
    policy.taskletLaunched(firstWorker, tasklet);
    launchedWorkers.add(firstWorkerId.get());

    for (int i = 0; i < maxDuplicate; i++) {
      // pendingTasklets.takeFirst() blocks until a Tasklet is put in the queue.
      final Tasklet nextTasklet = pendingTasklets.takeFirst();
      assertEquals("Should be same with the original Tasklet", tasklet, nextTasklet);

      final Optional<String> nextWorkerId = policy.trySchedule(nextTasklet);
      assertTrue("Should assign one worker", nextWorkerId.isPresent());

      assertFalse("Should be different from previous workers", launchedWorkers.contains(nextWorkerId.get()));
      policy.taskletLaunched(workers.get(nextWorkerId.get()), tasklet);
      launchedWorkers.add(nextWorkerId.get());
    }

    // When the Tasklets already have duplicate as maximum.
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final Future<Tasklet> notSupposed = executor.submit(new Callable<Tasklet>() {
      @Override
      public Tasklet call() throws Exception {
        return pendingTasklets.takeFirst();
      }
    });

    try {
      notSupposed.get(threshold + 2 * checkingPeriod, TimeUnit.SECONDS);
      fail();
    } catch (final TimeoutException e) {
      // Success
    }

    // Terminate one Tasklet
    policy.taskletCompleted(firstWorker, tasklet);

    assertTrue("Should be empty", policy.getScheduled().isEmpty());
  }

  /**
   * Test FirstFitSchedulingPolicy without preemption events.
   */
  @Test
  public void testFirstFitNoPreemption() throws Exception {
    final int workerCapacity = 1;
    final int numOfWorkers = 5;
    final FirstFitSchedulingPolicy policy = new FirstFitSchedulingPolicy(workerCapacity);

    // Add workers
    final Deque<VortexWorkerManager> workers = new ArrayDeque<>();
    for (int i = 0; i < numOfWorkers; i++) {
      final VortexWorkerManager worker = testUtil.newWorker();
      workers.addFirst(worker);
      policy.workerAdded(worker);
    }

    // Launch 1 tasklet per worker
    for (final VortexWorkerManager worker : workers) {
      final Tasklet tasklet = testUtil.newTasklet();
      assertEquals("This should be the first fit", worker.getId(), policy.trySchedule(tasklet).get());
      policy.taskletLaunched(worker, tasklet);
    }

    // When all workers are full...
    assertFalse("All workers should be full", policy.trySchedule(testUtil.newTasklet()).isPresent());
  }

  /**
   * Test FirstFitSchedulingPolicy with preemption events.
   */
  @Test
  public void testFirstFitPreemptions() throws Exception {
    final int workerCapacity = 1;
    final int numOfWorkers = 10;
    final FirstFitSchedulingPolicy policy = new FirstFitSchedulingPolicy(workerCapacity);

    // Add workers and make the odd ones full
    final ArrayDeque<VortexWorkerManager> evenWorkers = new ArrayDeque<>();
    for (int i = 0; i < numOfWorkers; i++) {
      final VortexWorkerManager worker = testUtil.newWorker();
      policy.workerAdded(worker);

      if (i % 2 == 1) {
        policy.taskletLaunched(worker, testUtil.newTasklet());
      } else {
        evenWorkers.addFirst(worker);
      }
    }

    // Check whether the policy returns even ones in order
    for (final VortexWorkerManager worker : evenWorkers) {
      final Tasklet tasklet = testUtil.newTasklet();
      assertEquals("This should be the first fit", worker.getId(), policy.trySchedule(tasklet).get());
      policy.taskletLaunched(worker, tasklet);
    }

    // When all workers are full...
    assertFalse("All workers should be full", policy.trySchedule(testUtil.newTasklet()).isPresent());
  }

  /**
   * Simple common tests.
   */
  private void commonPolicyTests(final SchedulingPolicy policy) {
    // Initial state
    assertFalse("No worker added yet", policy.trySchedule(testUtil.newTasklet()).isPresent());

    // One worker added
    final VortexWorkerManager worker = testUtil.newWorker();
    policy.workerAdded(worker);
    assertEquals("Only one worker exists", worker.getId(), policy.trySchedule(testUtil.newTasklet()).get());

    // One worker removed
    policy.workerRemoved(worker);
    assertFalse("No worker exists", policy.trySchedule(testUtil.newTasklet()).isPresent());
  }
}
