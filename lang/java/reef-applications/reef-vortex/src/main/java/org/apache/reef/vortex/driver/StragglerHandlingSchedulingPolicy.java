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

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.Alarm;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This scheduling policy assumes the Tasklets should run in the similar degree of running time.
 * If the the running time is n times longer than the average, a duplicate Tasklet is launched to
 * another worker. We use only results which finished earlier.
 */
public final class StragglerHandlingSchedulingPolicy implements SchedulingPolicy {
  private static final Logger LOG = Logger.getLogger(StragglerHandlingSchedulingPolicy.class.getName());
  private static final int MAX_DUPLICATE = 3;
  private static final int STRAGGLER_THRESHOLD_MILLIS = 3000;
  private static final int STRAGGLER_MONITOR_PERIOD_MILLIS = 1000;

  private final int workerCapacity;
  private final PendingTasklets pendingTasklets;
  private final Map<Integer, Set<Integer>> taskletIdToWorkers = new HashMap<>();
  private final Map<Tasklet, Long> taskletToLastLaunchTime = new HashMap<>();

  /**
   * Keep the load information for each worker.
   */
  private final HashMap<String, Integer> idLoadMap = new HashMap<>();

  /**
   * Keep the worker ids in an array for fast random selection.
   *
   * Add/removal from the array require O(n) complexity.
   */
  private final List<String> idList = new ArrayList<>();

  /**
   * The index of the next/first worker to check.
   */
  private int nextIndex = 0;

  @Inject
  StragglerHandlingSchedulingPolicy(@Parameter(VortexMasterConf.WorkerCapacity.class) final int capacity,
                                    final PendingTasklets pendingTasklets) {
    this.workerCapacity = capacity;
    this.pendingTasklets = pendingTasklets;
    Executors.newSingleThreadExecutor().execute(new StragglerMonitor());
  }

  @Override
  public Optional<String> trySchedule(final Tasklet tasklet) {
    for (int i = 0; i < idList.size(); i++) {
      final int index = (nextIndex + i) % idList.size();
      final String workerId = idList.get(index);

      if (idLoadMap.get(workerId) < workerCapacity) {
        synchronized (this) {
          if (taskletIdToWorkers.containsKey(tasklet.getId())) {
            final Set<Integer> workers = taskletIdToWorkers.get(tasklet.getId());
            if (workers.contains(idList.indexOf(workerId))) {
              continue;
            }
          } else {
            taskletIdToWorkers.put(tasklet.getId(), new HashSet<Integer>());
          }
        }
        nextIndex = (index + 1) % idList.size();
        return Optional.of(workerId);
      }
    }
    return Optional.empty();
  }

  @Override
  public void workerAdded(final VortexWorkerManager vortexWorker) {
    final String workerId = vortexWorker.getId();
    if (!idLoadMap.containsKey(workerId)) { // Ignore duplicate add.
      idLoadMap.put(workerId, 0);
      idList.add(nextIndex, workerId); // Prefer to schedule the new worker ASAP.
    }
  }

  @Override
  public void workerRemoved(final VortexWorkerManager vortexWorker) {
    final String workerId = vortexWorker.getId();
    if (idLoadMap.remove(workerId) != null) { // Ignore invalid removal.
      for (int i = 0; i < idList.size(); i++) { // This looping operation might degrade performance.
        if (idList.get(i).equals(workerId)) {
          idList.remove(i);

          if (i < nextIndex) {
            nextIndex--;
          } else if (nextIndex == idList.size()) {
            nextIndex = 0;
          }
          return;
        }
      }
    }
  }

  @Override
  public void taskletLaunched(final VortexWorkerManager vortexWorker, final Tasklet tasklet) {
    final String workerId = vortexWorker.getId();
    if (idLoadMap.containsKey(workerId)) {
      idLoadMap.put(workerId, Math.min(workerCapacity, idLoadMap.get(workerId) + 1));
    }

    synchronized (this) {
      final Set<Integer> workers = taskletIdToWorkers.get(tasklet.getId());
      workers.add(idList.indexOf(vortexWorker.getId()));
      taskletIdToWorkers.put(tasklet.getId(), workers);
      taskletToLastLaunchTime.put(tasklet, System.currentTimeMillis()); // update the last time
      LOG.log(Level.INFO, "Tasklet {0} is running on {1}, newly launched: {2}",
          new Object[]{tasklet.getId(), workers, idList.indexOf(vortexWorker.getId())});
    }
  }

  @Override
  public void taskletCompleted(final VortexWorkerManager vortexWorker, final Tasklet tasklet) {
    removeTasklet(vortexWorker.getId(), tasklet.getId());
  }

  @Override
  public void taskletFailed(final VortexWorkerManager vortexWorker, final Tasklet tasklet) {
    // Re-execute
    LOG.log(Level.WARNING, "Tasklet {0} failed in {1}", new Object[] {tasklet.getId(), vortexWorker.getId()});
  }

  // Hopefully this really cancels the execution of Tasklet.
  private void removeTasklet(final String workerId, final int taskletId) {
    if (idLoadMap.containsKey(workerId)) {
      idLoadMap.put(workerId, Math.max(0, idLoadMap.get(workerId) - 1));
    }

    synchronized (this) {
      if (taskletIdToWorkers.containsKey(taskletId)) {
        taskletIdToWorkers.remove(taskletId);
      }
    }
  }

  synchronized Map<Integer, Set<Integer>> getScheduled() {
    return taskletIdToWorkers;
  }

  int getMaxDuplicate() {
    return getMaxDuplicate();
  }

  final class StragglerAlarm implements EventHandler<Alarm> {
    private Tasklet tasklet;

    StragglerAlarm(final Tasklet tasklet) {
      this.tasklet = tasklet;
    }

    @Override
    public void onNext(final Alarm value) {
      // Only if the Tasklet is not complete
      // Check the number of trial?
      if (taskletIdToWorkers.containsKey(tasklet.getId())) {
        pendingTasklets.addFirst(tasklet);
        LOG.log(Level.INFO, "$Reschedule Tasklet {0}", tasklet.getId());
      } else {
        LOG.log(Level.INFO, "$Failed to reschedule Tasklet {0}, which has already finished", tasklet.getId());
      }
    }
  }

  /**
   * Monitors Straggler periodically, and duplicate the Tasklet to another if it seems to be a Straggler.
   */
  final class StragglerMonitor implements Runnable {
    @Override
    public void run() {
      while (true) {
        try {
          synchronized (this) {
            final long currentTime = System.currentTimeMillis();
            for (final Map.Entry<Tasklet, Long> entry : taskletToLastLaunchTime.entrySet()) {
              final Tasklet tasklet = entry.getKey();
              final int taskletId = tasklet.getId();
              final long startTime = entry.getValue();
              if (currentTime - startTime > STRAGGLER_THRESHOLD_MILLIS &&
                  taskletIdToWorkers.get(taskletId).size() < MAX_DUPLICATE) {
                LOG.log(Level.INFO, "Tasklet {0} seems to be a Straggler", taskletId);
                // Reschedule only if the maximum is not exceeded.
                pendingTasklets.addFirst(tasklet);
              }
            }
          }
          Thread.sleep(STRAGGLER_MONITOR_PERIOD_MILLIS);
        } catch (final InterruptedException e) {
          break;
        }
      }
    }
  }
}
