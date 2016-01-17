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

import org.apache.reef.util.Optional;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Deque;

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
    commonPolicyTests(new LocalityAwareSchedulingPolicy(10));
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
   * Test LocalityAwareSchedulingPolicy without preemption events.
   */
  @Test
  public void testLocalityAwareNoPreemption() throws Exception {
    final int workerCapacity = 2;
    final int numOfWorkers = 2;
    final SchedulingPolicy policy = new LocalityAwareSchedulingPolicy(workerCapacity);

    // Add workers
    final Deque<VortexWorkerManager> workers = new ArrayDeque<>();
    for (int i = 0; i < numOfWorkers; i++) {
      final VortexWorkerManager worker = testUtil.newWorker();
      workers.addFirst(worker);
      policy.workerAdded(worker);
    }

    final Tasklet tasklet1 = testUtil.newTaskletWithCache("key1");
    Optional<String> worker1 = policy.trySchedule(tasklet1);
    assertTrue(worker1.isPresent());

    final Tasklet tasklet2 = testUtil.newTaskletWithCache("key1");
    Optional<String> worker2 = policy.trySchedule(tasklet2);

    // Should be scheduled in the same worker
    assertEquals(worker1.get(), worker2.get());

    // Fill out worker1's capacity
    final VortexWorkerManager firstWorker = workers.getFirst();
    policy.taskletLaunched(firstWorker, tasklet1);
    policy.taskletLaunched(firstWorker, tasklet2);

    // Because the first worker has the tasklets as many as the capacity,
    // the scheduler chooses the other one.
    final Tasklet tasklet3 = testUtil.newTaskletWithCache("key1", "key2");
    Optional<String> worker3 = policy.trySchedule(tasklet3);
    assertNotEquals(worker1.get(), worker3.get());
    assertNotEquals(worker2.get(), worker3.get());

    // Since "key3" is not cached in anywhere, should be scheduled in a different worker
    final Tasklet tasklet4 = testUtil.newTaskletWithCache("key3");
    Optional<String> worker4 = policy.trySchedule(tasklet4);
    assertNotEquals(worker1.get(), worker4.get());
    assertNotEquals(worker2.get(), worker4.get());
    assertEquals(worker3.get(), worker4.get()); // Goes the same worker due to capacity, which has no meaning here.

    final Tasklet tasklet5 = testUtil.newTaskletWithCache("key2");
    Optional<String> worker5 = policy.trySchedule(tasklet5);
    assertEquals(worker3.get(), worker5.get()); // Goes the same worker due to capacity, which has no meaning here.
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
