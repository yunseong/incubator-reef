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
 * Request for cached data from Worker.
 * Note that the term Request is opposed to the Master-Worker protocol.
 */
public final class CachedDataRequest {
  private String keyId;

  /**
   * Create a report that requests the cached data.
   * @param keyId Key identifier to access the cache
   */
  public CachedDataRequest(final String keyId) {
    this.keyId = keyId;
  }

  /**
   * Get the key identifier assigned to the cache key.
   * @return The key identifier of the cache key to request to Master.
   */
  public String getKeyId() {
    return keyId;
  }
}
