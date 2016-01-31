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

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.util.Optional;
import org.apache.reef.vortex.api.*;
import org.apache.reef.vortex.common.*;
import org.apache.reef.vortex.common.exceptions.VortexCacheException;
import java.util.List;

/**
 * The heart of Vortex.
 * Processes various tasklet related events/requests coming from different components of the system.
 */
@Unstable
@DriverSide
@DefaultImplementation(DefaultVortexMaster.class)
public interface VortexMaster {
  /**
   * Submit a new Tasklet to be run sometime in the future, with an optional callback function on the result.
   */
  <TInput, TOutput> VortexFuture<TOutput>
      enqueueTasklet(final VortexFunction<TInput, TOutput> vortexFunction, final TInput input,
                     final Optional<FutureCallback<TOutput>> callback);

  /**
   * Submits aggregate-able Tasklets to be run sometime in the future, with an optional callback function on
   * the aggregation progress.
   */
  <TInput, TOutput> VortexAggregateFuture<TInput, TOutput>
      enqueueTasklets(final VortexAggregateFunction<TOutput> aggregateFunction,
                      final VortexFunction<TInput, TOutput> vortexFunction,
                      final VortexAggregatePolicy policy,
                      final List<TInput> inputs,
                      final Optional<FutureCallback<AggregateResult<TInput, TOutput>>> callback);

  /**
   * Call this when a Tasklet is to be cancelled.
   * @param mayInterruptIfRunning if true, will attempt to cancel running Tasklets; otherwise will only
   *                              prevent a pending Tasklet from running.
   * @param taskletId the ID of the Tasklet.
   */
  void cancelTasklet(final boolean mayInterruptIfRunning, final int taskletId);

  /**
   * Call this when a new worker is up and running.
   */
  void workerAllocated(final VortexWorkerManager vortexWorkerManager);

  /**
   * Call this when a worker is preempted.
   */
  void workerPreempted(final String id);

  /**
   * Call this when a worker has reported back.
   */
  void workerReported(final String workerId, final WorkerReport workerReport);

  /**
   * Call this when user caches the data.
   * @param keyId Unique identifier of the data in the cache
   * @param data Data to cache
   * @param <T> Type of the data
   * @return The key with which the data is accessible in the Worker
   * @throws VortexCacheException If the keyName is registered already in the cache.
   */
  <T> MasterCacheKey<T> cache(final String keyId, final T data, final Codec<T> codec)
      throws VortexCacheException;

  /**
   * Call this when user wants to cache the data in the HDFS.
   * @param path Path of the file to cache
   * @param numSplit Number of splits (Partial data is loaded if the requested number is smaller than the num of blocks)
   * @param parser Parser that transforms the loaded data into the real data to use
   * @param <T> Data type to be used in the user code
   * @return Array of the Cache key for accessing the data in Worker.
   */
  <T> HdfsCacheKey<T>[] cache(final String path, final int numSplit, final VortexParser<?, T> parser);

  /**
   * Invalidate a data associated with the cache key.
   * @param key
   */
  void invalidate(final CacheKey key);

  /**
   * Call this when a worker requests the cached data.
   * Retrieve the data from the Cache, and send it to the Worker who requested.
   * @param workerId Worker id to send the data
   * @param keyId key id of the cache key assigned to the data
   * @throws VortexCacheException If the data is not found in the cache.
   */
  void dataRequested(final String workerId, final String keyId)
      throws VortexCacheException;

  /**
   * Release all resources and shut down.
   */
  void terminate();
}
