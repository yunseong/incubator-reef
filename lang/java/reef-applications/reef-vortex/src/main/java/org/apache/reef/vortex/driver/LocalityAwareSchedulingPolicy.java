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

import net.jcip.annotations.NotThreadSafe;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.apache.reef.vortex.common.CacheKey;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Schedule Tasklets with best effort for data locality by bookkeeping the location of cached data.
 * If cached data is not loaded, workers' capacity is considered as a second factor.
 */
@NotThreadSafe
class LocalityAwareSchedulingPolicy implements SchedulingPolicy {

  private final int workerCapacity;

  /**
   * Keep the load information for each worker.
   */
  private final HashMap<String, Integer> idLoadMap = new HashMap<>();

  /**
   * Keep the location for each cached data.
   * There could be multiple workers who have the same cache data.
   */
  private final HashMap<CacheKey, Collection<String>> keyWorkersMap = new HashMap<>();

  /**
   * Keep the cache keys for each worker.
   * This reverse mapping is used to remove keys when a worker is removed.
   */
  private final HashMap<String, Collection<CacheKey>> workerKeysMap = new HashMap<>();

  /**
   * A linked list for a circular buffer of worker ids for search in a round-robin fashion.
   */
  private final List<String> idList = new ArrayList<>();

  /**
   * The index of the next/first worker to check.
   */
  private int nextIndex = 0;


  @Inject
  LocalityAwareSchedulingPolicy(@Parameter(VortexMasterConf.WorkerCapacity.class) final int capacity) {
    this.workerCapacity = capacity;
  }

  /**
   * Checking from nextIndex, choose the first worker that fits to schedule the tasklet onto.
   * @param tasklet to schedule
   * @return the next worker that has enough resources for the tasklet
   */
  @Override
  public Optional<String> trySchedule(final Tasklet tasklet) {
    final List<CacheKey> keys = tasklet.getCachedKeys();
    for (final CacheKey key : keys) {

      if (keyWorkersMap.containsKey(key)) {
        // If already cached
        for (final String workerId : keyWorkersMap.get(key)) {
          if (idLoadMap.containsKey(workerId) && idLoadMap.get(workerId) < workerCapacity) {
            // There could be a race condition that worker remains right after the preemption.
            // Only if the worker has enough capacity
            if (workerId.equals(idList.get(nextIndex % idList.size()))) {
              // To prevent workload skew, move the cursor to the next
              nextIndex = (nextIndex + 1) % idList.size();
            }
            return Optional.of(workerId);
          }
        }
      }
    }

    // If any worker does not have the key or there is no worker that have capacity to run additional tasklets,
    // pick up a worker in round-robin fashion
    final Optional<String> picked = pickWorker();
    if (picked.isPresent()) {
      updateKeyLocations(keys, picked.get());
    }
    return picked;
  }

  /**
   * Update key locations as a result of worker scheduling.
   * @param workerId
   */
  private void updateKeyLocations(final Collection<CacheKey> keys, final String workerId) {
    for (final CacheKey key : keys) {
      if (!keyWorkersMap.containsKey(key)) {
        keyWorkersMap.put(key, new HashSet<String>());
      }
      keyWorkersMap.get(key).add(workerId);
    }

    if (!workerKeysMap.containsKey(workerId)) {
      workerKeysMap.put(workerId, new HashSet<CacheKey>());
    }

    final Collection<CacheKey> workerKeys = workerKeysMap.get(workerId);
    workerKeys.addAll(keys);
  }


  /**
   * Checking from nextIndex, choose the first worker that fits to schedule the tasklet onto.
   * @return the next worker that has enough resources for the tasklet
   */
  private Optional<String> pickWorker() {
    for (int i = 0; i < idList.size(); i++) {
      final int index = (nextIndex + i) % idList.size();
      final String workerId = idList.get(index);

      if (idLoadMap.get(workerId) < workerCapacity) {
        nextIndex = (index + 1) % idList.size();
        return Optional.of(workerId);
      }
    }
    return Optional.empty();
  }

  /**
   * @param vortexWorker added
   */
  @Override
  public void workerAdded(final VortexWorkerManager vortexWorker) {
    final String workerId = vortexWorker.getId();
    if (!idLoadMap.containsKey(workerId)) { // Ignore duplicate add.
      idLoadMap.put(workerId, 0);
      idList.add(nextIndex, workerId); // Prefer to schedule the new worker ASAP.
    }
  }

  /**
   * @param vortexWorker removed
   */
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

    // Need to check performance problem.
    final Collection<CacheKey> keys = workerKeysMap.remove(workerId);
    if (keys != null) {
      for (final CacheKey key : keys) {
        if (keyWorkersMap.containsKey(key)) {
          keyWorkersMap.get(key).remove(workerId);
        }
      }
    }
  }

  /**
   * @param vortexWorker that the tasklet was launched onto
   * @param tasklet launched
   */
  @Override
  public void taskletLaunched(final VortexWorkerManager vortexWorker, final Tasklet tasklet) {
    final String workerId = vortexWorker.getId();
    if (idLoadMap.containsKey(workerId)) {
      idLoadMap.put(workerId, Math.min(workerCapacity, idLoadMap.get(workerId) + 1));
    }

    updateKeyLocations(tasklet.getCachedKeys(), workerId);
  }

  @Override
  public void taskletsDone(final VortexWorkerManager vortexWorker, final List<Tasklet> tasklets) {
    removeTasklet(vortexWorker.getId(), tasklets);
  }

  private void removeTasklet(final String workerId, final List<Tasklet> tasklets) {
    if (idLoadMap.containsKey(workerId)) {
      idLoadMap.put(workerId, Math.max(0, idLoadMap.get(workerId) - tasklets.size()));
    }
  }
}
