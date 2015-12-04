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

import org.apache.htrace.TraceInfo;

import java.io.Serializable;

/**
 * The message for Worker to request the cache data to Master.
 */
// TODO: The naming needs to be fixed.
public class CacheDataRequest<T> implements WorkerReport {
  private MasterCacheKey<T> cacheKey;
  private long traceId;
  private long spanId;

  public CacheDataRequest(final MasterCacheKey<T> cacheKey, final TraceInfo traceInfo) {
    this.cacheKey = cacheKey;
    this.traceId = traceInfo.traceId;
    this.spanId = traceInfo.spanId;
  }

  @Override
  public WorkerReportType getType() {
    return WorkerReportType.CacheRequest;
  }

  /**
   * @return The key of the data to request to Master.
   */
  public MasterCacheKey<T> getCacheKey() {
    return cacheKey;
  }

  /**
   * @return TraceInfo that this request contains.
   */
  public TraceInfo getTraceInfo() {
    return new TraceInfo(traceId, spanId);
  }
}
