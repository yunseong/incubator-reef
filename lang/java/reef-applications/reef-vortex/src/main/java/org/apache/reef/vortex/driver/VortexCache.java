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

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.vortex.common.CacheKey;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The cache API in the Master side.
 */
@DriverSide
public final class VortexCache {
  private final ConcurrentHashMap<CacheKey, Serializable> map = new ConcurrentHashMap<>();

  @Inject
  private VortexCache() {
  }

  /**
   * Put the data in cache.
   * @param data Data to cache.
   * @param <T> Type of the data.
   * @return Key that encapsulates the type of data. The data can be retrieved from the Worker cache with this key.
   */
  public <T extends Serializable> CacheKey<T> put(final T data) {
    final CacheKey<T> key = new CacheKey<>();
    map.put(key, data);
    return key;
  }

  /**
   * Get the data that is mapped to the given key.
   * @param key Key to retrieve the data.
   * @param <T> Type of the data.
   * @return Data retrieved from the cache.
   */
  public <T extends Serializable> T get(final CacheKey<T> key) {
    return (T) map.get(key);
  }
}
