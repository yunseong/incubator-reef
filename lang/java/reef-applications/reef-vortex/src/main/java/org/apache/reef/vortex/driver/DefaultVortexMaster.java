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
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.util.Optional;
import org.apache.reef.vortex.api.VortexFunction;
import org.apache.reef.vortex.api.VortexFuture;
import org.apache.reef.vortex.common.CacheKey;
import org.apache.reef.vortex.common.exceptions.VortexCacheException;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Default implementation of VortexMaster.
 * Uses two thread-safe data structures(pendingTasklets, runningWorkers) in implementing VortexMaster interface.
 */
@ThreadSafe
@DriverSide
final class DefaultVortexMaster implements VortexMaster {
  private final AtomicInteger taskletIdCounter = new AtomicInteger();
  private final RunningWorkers runningWorkers;
  private final PendingTasklets pendingTasklets;
  // TODO This should be replaced by Guava
  private final Map<String, Serializable> cacheMap = new HashMap<>();

  /**
   * @param runningWorkers for managing all running workers.
   */
  @Inject
  DefaultVortexMaster(final RunningWorkers runningWorkers,
                      final PendingTasklets pendingTasklets) {
    this.runningWorkers = runningWorkers;
    this.pendingTasklets = pendingTasklets;
  }

  /**
   * Add a new tasklet to pendingTasklets.
   */
  @Override
  public <TInput extends Serializable, TOutput extends Serializable> VortexFuture<TOutput>
      enqueueTasklet(final VortexFunction<TInput, TOutput> function, final TInput input) {
    // TODO[REEF-500]: Simple duplicate Vortex Tasklet launch.
    final VortexFuture<TOutput> vortexFuture = new VortexFuture<>();
    this.pendingTasklets.addLast(new Tasklet<>(taskletIdCounter.getAndIncrement(), function, input, vortexFuture));
    return vortexFuture;
  }

  /**
   * Add a new worker to runningWorkers.
   */
  @Override
  public void workerAllocated(final VortexWorkerManager vortexWorkerManager) {
    runningWorkers.addWorker(vortexWorkerManager);
  }

  /**
   * Remove the worker from runningWorkers and add back the lost tasklets to pendingTasklets.
   */
  @Override
  public void workerPreempted(final String id) {
    final Optional<Collection<Tasklet>> preemptedTasklets = runningWorkers.removeWorker(id);
    if (preemptedTasklets.isPresent()) {
      for (final Tasklet tasklet : preemptedTasklets.get()) {
        pendingTasklets.addFirst(tasklet);
      }
    }
  }

  /**
   * Notify task completion to runningWorkers.
   */
  @Override
  public void taskletCompleted(final String workerId,
                               final int taskletId,
                               final Serializable result) {
    runningWorkers.completeTasklet(workerId, taskletId, result);
  }

  /**
   * Notify task failure to runningWorkers.
   */
  @Override
  public void taskletErrored(final String workerId, final int taskletId, final Exception exception) {
    runningWorkers.errorTasklet(workerId, taskletId, exception);
  }

  @Override
  public <T extends Serializable> CacheKey cache(final String keyName, @Nonnull final T data)
      throws VortexCacheException {
    synchronized (cacheMap) {
      if (cacheMap.containsKey(keyName)) {
        throw new VortexCacheException("The keyName " + keyName + "is already used.");
      }

      final CacheKey key = new CacheKey(keyName);
      cacheMap.put(keyName, data);
      return key;
    }
  }

  @Override
  public void dataRequested(final String workerId, final CacheKey cacheKey) throws VortexCacheException {
    synchronized (cacheMap) {
      final String keyName = cacheKey.getName();
      if (!cacheMap.containsKey(keyName)) {
        throw new VortexCacheException("The entity does not exist for the key : " + cacheKey);
      }
      final Serializable data = cacheMap.get(keyName);
      runningWorkers.sendCacheData(workerId, cacheKey, data);
    }
  }

  /**
   * Terminate the job.
   */
  @Override
  public void terminate() {
    runningWorkers.terminate();
  }
}
