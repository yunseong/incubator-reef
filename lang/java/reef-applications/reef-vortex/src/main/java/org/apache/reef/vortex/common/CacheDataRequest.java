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

import java.io.Serializable;

/**
 * The message for Worker to request the cache data to Master.
 */
// TODO: The naming needs to be fixed.
public class CacheDataRequest<T extends Serializable> implements WorkerReport {
  private CacheKey<T> cacheKey;

  public CacheDataRequest(final CacheKey<T> cacheKey) {
    this.cacheKey = cacheKey;
  }

  @Override
  public WorkerReportType getType() {
    return WorkerReportType.CacheRequest;
  }

  /**
   * @return The key of the data to request to Master.
   */
  public CacheKey<T> getCacheKey() {
    return cacheKey;
  }
}
