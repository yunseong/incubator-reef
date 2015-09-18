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
package org.apache.reef.vortex.evaluator;

import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.util.cache.Cache;
import org.apache.reef.util.cache.CacheImpl;
import org.apache.reef.util.cache.SystemTime;
import org.apache.reef.vortex.common.CacheKey;
import org.apache.reef.vortex.common.exceptions.VortexCacheException;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * VortexCache based on REEF's cache. {@link org.apache.reef.util.cache.Cache}
 */
final class VortexCache {
  private final Cache<CacheKey<? extends Serializable>, Serializable> cache;
  private final InjectionFuture<VortexWorker> vortexWorker;
  private PendingData pendingData = new PendingData();

  private static final long CACHE_TIMEOUT_MS = 1000 * 1000; // Timeout: 1000s

  @Inject
  private VortexCache(final InjectionFuture<VortexWorker> vortexWorker) {
    this.vortexWorker = vortexWorker;
    this.cache = new CacheImpl<>(new SystemTime(), CACHE_TIMEOUT_MS); // TODO Replace this with Guava
  }

  /**
   * Get the data from the cache.
   * @param key Key of the data.
   * @param <T> Type of the data.
   * @return Data assigned to the given key.
   * @throws VortexCacheException If a failure occurred while fetching the data.
   */
  public <T extends Serializable> T get(final CacheKey<T> key) throws VortexCacheException {
    try {
      return (T) cache.get(key, new Callable<Serializable>() {
        @Override
        public T call() throws Exception {
          vortexWorker.get().sendDataRequest(key);
          synchronized (pendingData) {
            pendingData.wait(CACHE_TIMEOUT_MS);
          }
          return (T) pendingData.data;
        }
      });
    } catch (ExecutionException e) {
      throw new VortexCacheException("Failed to fetch the data for key: " + key, e);
    }
  }

  /**
   * Called when the data arrives in the VortexWorker.
   * @param key Key of the data
   * @param data Data that has arrived from the Master
   * @param <T> Type of the data
   */
  public <T extends Serializable> void onDataArrived(final CacheKey<T> key, final T data) {
    synchronized (pendingData) {
      pendingData.data = data;
      pendingData.notify();
    }
  }

  /**
   * Encapsulates the data which is being loaded to the cache.
   */
  final class PendingData {
    private Serializable data;

    private PendingData() {
    }
  }
}
