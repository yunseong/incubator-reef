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

import org.apache.reef.util.cache.Cache;
import org.apache.reef.vortex.common.CacheKey;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * Implementation of VortexCache based on REEF's cache. {@link org.apache.reef.util.cache.Cache}
 */
public class DefaultVortexCache implements VortexCache {
  private final Cache<CacheKey<? extends Serializable>, Serializable> cache;
  private final VortexWorker vortexWorker;
  private final ConcurrentHashMap<CacheKey, Serializable> waitingData = new ConcurrentHashMap<>();

  @Inject
  private DefaultVortexCache(final VortexWorker vortexWorker,
                             final Cache<CacheKey<? extends Serializable>, Serializable> cache) {
    this.vortexWorker = vortexWorker;
    this.cache = cache;
  }

  @Override
  public <T extends Serializable> T get(final CacheKey<T> key) {
    try {
      return (T) cache.get(key, new Callable<Serializable>() {
        @Override
        public T call() throws Exception {
          vortexWorker.sendCacheDataRequest(key);
          synchronized (waitingData) {
            while(!waitingData.contains(key)) {
              waitingData.wait(); // TODO: Timeout?
            }
          }
          return (T) waitingData.remove(key);
        }
      });
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Called when the data arrives in the VortexWorker
   * @param key Key of the data
   * @param data Data to put in the cache
   */
  public <T extends Serializable> void onDataArrived(final CacheKey<T> key, final T data) {
    synchronized (waitingData) {
      waitingData.put(key, data);
      waitingData.notify();
    }
  }
}
