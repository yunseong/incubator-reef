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
 * Informs that the data was sent to Worker.
 * @param <T> Type of the data.
 */
// TODO Fix the naming: note that this is not a request actually.
public final class CacheSentRequest<T extends Serializable> {
  private CacheKey<T> cacheKey;
  private T data;

  public CacheSentRequest() {
  }

  /**
   * @param key Key of the data.
   * @param data Data that was requested to send.
   */
  public CacheSentRequest(final CacheKey<T> key, final T data) {
    this.cacheKey = key;
    this.data = data;
  }

  /**
   * @return Key of the data.
   */
  public CacheKey<T> getCacheKey() {
    return cacheKey;
  }

  /**
   * @return Data that was requested to send.
   */
  public T getData() {
    return data;
  }

  @Override
  public String toString() {
    return "CacheSentRequest{" +
        "cacheKey=" + cacheKey +
        ", data=" + data +
        '}';
  }
}
