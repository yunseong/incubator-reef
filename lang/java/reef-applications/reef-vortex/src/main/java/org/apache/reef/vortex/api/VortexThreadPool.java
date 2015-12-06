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
import org.apache.reef.util.Optional;
import org.apache.reef.vortex.common.CacheKey;
import org.apache.reef.vortex.common.exceptions.VortexCacheException;
import org.apache.reef.vortex.driver.VortexMaster;
import org.apache.reef.wake.EventHandler;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.io.Serializable;

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
   * Put the data in cache by calling {@link VortexMaster#cache(String, Serializable)}.
   * @param name Name to distinguish the data, which should be unique.
   * @param data Data to cache.
   * @param <T> Type of the data.
   * @return Key that is used to access the data in Workers {@link org.apache.reef.vortex.evaluator.VortexCache}.
   * @throws VortexCacheException If the keyName is registered already in the cache.
   */
  public <T extends Serializable> CacheKey<T> cache(final String name, @Nonnull final T data)
      throws VortexCacheException {
    return vortexMaster.cache(name, data);
  }

  /**
   * @param function to run on Vortex
   * @param input of the function
   * @param <TInput> input type
   * @param <TOutput> output type
   * @return VortexFuture for tracking execution progress
   */
  public <TInput, TOutput extends Serializable> VortexFuture<TOutput>
      submit(final VortexFunction<TInput, TOutput> function, final TInput input) {
    return vortexMaster.enqueueTasklet(function, input, Optional.<EventHandler<TOutput>>empty());
  }

  /**
   * @param function to run on Vortex
   * @param input of the function
   * @param callback of the function
   * @param <TInput> input type
   * @param <TOutput> output type
   * @return VortexFuture for tracking execution progress
   */
  public <TInput extends Serializable, TOutput extends Serializable> VortexFuture<TOutput>
      submit(final VortexFunction<TInput, TOutput> function, final TInput input,
             final EventHandler<TOutput> callback) {
    return vortexMaster.enqueueTasklet(function, input, Optional.of(callback));
  }
}
