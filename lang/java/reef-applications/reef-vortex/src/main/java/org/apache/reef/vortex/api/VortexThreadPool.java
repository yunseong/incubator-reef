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
package org.apache.reef.vortex.api;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.util.Optional;
import org.apache.reef.vortex.common.CacheKey;
import org.apache.reef.vortex.common.HdfsCacheKey;
import org.apache.reef.vortex.common.VortexParser;
import org.apache.reef.vortex.common.exceptions.VortexCacheException;
import org.apache.reef.vortex.driver.VortexMaster;

import javax.inject.Inject;

/**
 * Distributed thread pool.
 */
@Unstable
public final class VortexThreadPool {
  private final VortexMaster vortexMaster;

  @Inject
  private VortexThreadPool(final VortexMaster vortexMaster) {
    this.vortexMaster = vortexMaster;
  }

  /**
   * @param function to run on Vortex
   * @param input of the function
   * @param <TInput> input type
   * @param <TOutput> output type
   * @return VortexFuture for tracking execution progress
   */
  public <TInput, TOutput> VortexFuture<TOutput>
      submit(final VortexFunction<TInput, TOutput> function, final TInput input) {
    return vortexMaster.enqueueTasklet(function, input, Optional.<FutureCallback<TOutput>>empty());
  }

  /**
   * @param function to run on Vortex
   * @param input of the function
   * @param callback of the function
   * @param <TInput> input type
   * @param <TOutput> output type
   * @return VortexFuture for tracking execution progress
   */
  public <TInput, TOutput> VortexFuture<TOutput>
      submit(final VortexFunction<TInput, TOutput> function, final TInput input,
             final FutureCallback<TOutput> callback) {
    return vortexMaster.enqueueTasklet(function, input, Optional.of(callback));
  }

  /**
   * Put the data in cache by calling {@link VortexMaster#cache(String, Object, Codec)}.
   * @param name Name to distinguish the data, which should be unique.
   * @param data Data to cache.
   * @param <T> Type of the data.
   * @return Key that is used to access the data in Workers {@link org.apache.reef.vortex.evaluator.VortexCache}.
   * @throws VortexCacheException If the keyName is registered already in the cache.
   */
  public <T> CacheKey<T> cache(final String name, final T data, final Codec<T> codec)
      throws VortexCacheException {
    return vortexMaster.cache(name, data, codec);
  }

  /**
   * Let the data from HDFS cached in Workers. Instead of caching the actual data in Master's heap space,
   * just metadata for loading the data is stored.
   * @param path Path of the file in HDFS to cache in workers.
   * @param numSplit Number of splits (Partial data is loaded if the requested number is smaller than the num of blocks)
   * @param parser Parser that transforms the loaded data into the real data to use
   * @param <T> Data type to be used in the user code
   * @return Array of the Cache key for accessing the data in Worker.
   */
  public <T> HdfsCacheKey<T>[] cache(final String path, final int numSplit, final VortexParser<?, T> parser) {
    return vortexMaster.cache(path, numSplit, parser);
  }

  public void cleanCache() {
    vortexMaster.cleanCache();
  }
}