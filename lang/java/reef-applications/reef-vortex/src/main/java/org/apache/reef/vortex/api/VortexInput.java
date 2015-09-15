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

import org.apache.reef.vortex.common.CacheKey;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * Typed input. User can specify which data will be cached.
 */
public interface VortexInput extends Serializable {
  /**
   * To cache data, Users should put the data in {@link VortexStart#start(VortexThreadPool)},
   * and let Vortex know the key retrieved in {@link org.apache.reef.vortex.driver.VortexCache}.
   * @param <T> Type of the data to cache.
   * @return The key returned from {@link org.apache.reef.vortex.driver.VortexCache#put(Serializable)}.
   * Returns {@code null} if no data is cached.
   */
  @Nullable
  <T extends Serializable> CacheKey<T> getCachedKey();

  /**
   * This method is the counterpart of {@code getCachedKey()}. If the data is returned here,
   * Vortex ships the data with the Function code when submits a Tasklet.
   * @param <T> Type of the data.
   * @return The data that users do not want to cache. Returns {@code null} if there is no data that is not cached.
   */
  @Nullable
  <T extends Serializable> T getNotCachedData();
}

