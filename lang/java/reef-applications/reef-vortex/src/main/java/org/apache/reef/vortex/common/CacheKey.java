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
package org.apache.reef.vortex.common;

/**
 * CacheKey is issued in Master's user code. Tasklets use this key to access
 * the data from Worker cache. Each data is distinguished by cache key's identifier.
 */
public interface CacheKey<T> {
  enum CacheKeyType {
    MASTER
  }

  /**
   * Get identifier assigned to this cache key.
   * @return Identifier of the cache key
   */
  String getId();

  /**
   * Type of the cache key. Caching in the Vortex Master's memory is only supported.
   * @return Type of the cache key
   */
  CacheKeyType getType();
}