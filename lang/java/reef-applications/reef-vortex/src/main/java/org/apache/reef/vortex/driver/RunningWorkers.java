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

import net.jcip.annotations.ThreadSafe;

import org.apache.htrace.TraceInfo;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.util.Optional;
import org.apache.reef.vortex.common.MasterCacheKey;

import javax.inject.Inject;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Keeps track of all running VortexWorkers and Tasklets.
 * Upon Tasklet launch request, randomly schedules it to a VortexWorkerManager.
 */
@ThreadSafe
@DriverSide
final class RunningWorkers {
  // RunningWorkers and its locks
  private final HashMap<String, VortexWorkerManager> runningWorkers = new HashMap<>(); // Running workers/tasklets
  private final Lock lock = new ReentrantLock();
  private final Condition noWorkerOrResource = lock.newCondition();

  // To keep track of workers that are preempted before acknowledged
  private final Set<String> removedBeforeAddedWorkers = new HashSet<>();

  // Terminated
  private boolean terminated = false;

  // Scheduling policy
  private final SchedulingPolicy schedulingPolicy;

  /**
   * RunningWorkers constructor.
   */
  @Inject
  RunningWorkers(final SchedulingPolicy schedulingPolicy) {
    this.schedulingPolicy = schedulingPolicy;
  }

  /**
   * Concurrency: Called by multiple threads.
   * Parameter: Called exactly once per vortexWorkerManager.
   */
  void addWorker(final VortexWorkerManager vortexWorkerManager) {
    lock.lock();
    try {
      if (!terminated) {
        if (!removedBeforeAddedWorkers.contains(vortexWorkerManager.getId())) {
          this.runningWorkers.put(vortexWorkerManager.getId(), vortexWorkerManager);
          this.schedulingPolicy.workerAdded(vortexWorkerManager);

          // Notify (possibly) waiting scheduler
          noWorkerOrResource.signal();
        }
      } else {
        // Terminate the worker
        vortexWorkerManager.terminate();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Concurrency: Called by multiple threads.
   * Parameter: Called exactly once per id.
   */
  Optional<Collection<Tasklet>> removeWorker(final String id) {
    lock.lock();
    try {
      if (!terminated) {
        final VortexWorkerManager vortexWorkerManager = this.runningWorkers.remove(id);
        if (vortexWorkerManager != null) {
          this.schedulingPolicy.workerRemoved(vortexWorkerManager);
          return Optional.ofNullable(vortexWorkerManager.removed());
        } else {
          // Called before addWorker (e.g. RM preempted the resource before the Evaluator started)
          removedBeforeAddedWorkers.add(id);
          return Optional.empty();
        }
      } else {
        // No need to return anything since it is terminated
        return Optional.empty();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Concurrency: Called by single scheduler thread.
   * Parameter: Same tasklet can be launched multiple times.
   */
  void launchTasklet(final Tasklet tasklet) {
    lock.lock();
    try {
      if (!terminated) {
        Optional<String> workerId;
        while(true) {
          workerId = schedulingPolicy.trySchedule(tasklet);
          if (!workerId.isPresent()) {
            try {
              noWorkerOrResource.await();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          } else {
            break;
          }
        }

        final VortexWorkerManager vortexWorkerManager = runningWorkers.get(workerId.get());
        vortexWorkerManager.launchTasklet(tasklet);
        schedulingPolicy.taskletLaunched(vortexWorkerManager, tasklet);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Concurrency: Called by multiple threads.
   * Parameter: Same arguments can come in multiple times.
   * (e.g. preemption message coming before tasklet completion message multiple times)
   */
  void completeTasklet(final String workerId,
                       final int taskletId,
                       final Serializable result) {
    lock.lock();
    try {
      if (!terminated) {
        if (runningWorkers.containsKey(workerId)) { // Preemption can come before
          final VortexWorkerManager worker = this.runningWorkers.get(workerId);
          final Tasklet tasklet = worker.taskletCompleted(taskletId, result);
          this.schedulingPolicy.taskletCompleted(worker, tasklet);

          // Notify (possibly) waiting scheduler
          noWorkerOrResource.signal();
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Concurrency: Called by multiple threads.
   * Parameter: Same arguments can come in multiple times.
   * (e.g. preemption message coming before tasklet error message multiple times)
   */
  void errorTasklet(final String workerId,
                    final int taskletId,
                    final Exception exception) {
    lock.lock();
    try {
      if (!terminated) {
        if (runningWorkers.containsKey(workerId)) { // Preemption can come before
          final VortexWorkerManager worker = this.runningWorkers.get(workerId);
          final Tasklet tasklet = worker.taskletThrewException(taskletId, exception);
          this.schedulingPolicy.taskletFailed(worker, tasklet);

          // Notify (possibly) waiting scheduler
          noWorkerOrResource.signal();
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Send the cache data to Worker.
   * @param workerId Worker who requested the cache data.
   * @param cacheKey Key that is assigned to the data.
   * @param <T> Type of the data.
   */
  <T extends Serializable> void sendCacheData(final String workerId,
                                              final MasterCacheKey<T> cacheKey,
                                              final T data,
                                              final TraceInfo traceInfo) {
    if (isWorkerRunning(workerId)) {
      this.runningWorkers.get(workerId).sendCacheData(cacheKey, data, traceInfo);
      // TODO: schedulingPolicy#cached for bookkeeping cache location
    } else {
      throw new RuntimeException("Worker is not running");
    }
  }

  void terminate() {
    lock.lock();
    try {
      if (!terminated) {
        terminated = true;
        for (final VortexWorkerManager vortexWorkerManager : runningWorkers.values()) {
          vortexWorkerManager.terminate();
          schedulingPolicy.workerRemoved(vortexWorkerManager);
        }
        runningWorkers.clear();
      } else {
        throw new RuntimeException("Attempting to terminate an already terminated RunningWorkers");
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Send the serialized request to Worker.
   */
  <T extends Serializable> void sendCacheData(final String workerId,
                                              final byte[] serializedRequest,
                                              final TraceInfo traceInfo) {
    if (isWorkerRunning(workerId)) {
      this.runningWorkers.get(workerId).sendCacheData(serializedRequest, traceInfo);
      // TODO: schedulingPolicy#cached for bookkeeping cache location
    } else {
      throw new RuntimeException("Worker is not running");
    }
  }

  boolean isTerminated() {
    return terminated;
  }

  ///////////////////////////////////////// For Tests Only

  /**
   * For unit tests to check whether the worker is running.
   */
  boolean isWorkerRunning(final String workerId) {
    return runningWorkers.containsKey(workerId);
  }

  /**
   * For unit tests to see where a tasklet is scheduled to.
   * @param taskletId id of the tasklet in question
   * @return id of the worker (null if the tasklet was not scheduled to any worker)
   */
  String getWhereTaskletWasScheduledTo(final int taskletId) {
    for (final Map.Entry<String, VortexWorkerManager> entry : runningWorkers.entrySet()) {
      final String workerId = entry.getKey();
      final VortexWorkerManager vortexWorkerManager = entry.getValue();
      if (vortexWorkerManager.containsTasklet(taskletId)) {
        return workerId;
      }
    }
    return null;
  }
}
