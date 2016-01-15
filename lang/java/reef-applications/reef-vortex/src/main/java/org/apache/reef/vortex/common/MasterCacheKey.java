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

import org.apache.reef.io.serialization.Codec;

/**
 * Key used to get the data from the Worker cache
 * Users should assign unique name to distinguish the keys.
 * NOTE: I couldn't find a way for CacheKey to be serialized/deserialized in input/output.
 *      For LR, I took a workaround of using Kryo.
 */
public final class MasterCacheKey<T> implements CacheKey<T> {
  private String id;
  private Codec<T> codec;

  private MasterCacheKey(){
  }

  /**
   * Create a MasterCacheKey. Identifier and Codec for the data type should be provided.
   * @param id Identifier which is assigned to this key
   * @param codec Codec to serialize/deserialize the data
   */
  public MasterCacheKey(final String id, final Codec<T> codec) {
    this.id = id;
    this.codec = codec;
  }

  /**
   * @return Identifier of the key.
   */
  public String getId() {
    return id;
  }

  /**
   * @return Codec to serialize/deserialize the actual data.
   */
  public Codec<T> getCodec() {
    return codec;
  }

  @Override
  public String toString() {
    return "MasterCacheKey{name='" + id + "'}";
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final MasterCacheKey<?> cacheKey = (MasterCacheKey<?>) o;

    return id.equals(cacheKey.id);
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public CacheKeyType getType() {
    return CacheKeyType.MASTER;
  }
}