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
 * Response for the cached data request.
 * Note that the term Response does not match well with Master-Worker protocol.
 */
public class CachedDataResponse implements VortexRequest {
  private String keyId;
  private byte[] serializedData;

  /**
   * Constructor of CachedDataResponse.
   * @param keyId Identifier of the CacheKey
   * @param serializedData Cached data in a serialized form
   */
  public CachedDataResponse(final String keyId, final byte[] serializedData) {
    this.keyId = keyId;
    this.serializedData = serializedData;
  }

  /**
   * @return Key identifier of the data.
   */
  public String getKeyId() {
    return keyId;
  }

  /**
   * @return Data that was requested to send.
   */
  public byte[] getSerializedData() {
    return serializedData;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder()
        .append("CachedDataResponse{").append("key=").append(keyId).append('}');
    return sb.toString();
  }

  @Override
  public RequestType getType() {
    return RequestType.CachedDataResponse;
  }
}
