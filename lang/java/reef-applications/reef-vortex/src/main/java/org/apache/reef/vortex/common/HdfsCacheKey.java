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
 * Key used to get the data from the Worker cache
 * Users should assign unique name to distinguish the keys.
 * NOTE: I couldn't find a way for CacheKey to be serialized/deserialized in input/output.
 *      For LR, I took a workaround of using Kryo.
 */
public final class HdfsCacheKey<T> implements CacheKey<T> {
  private String serializedInputSplit;
  private String path;
  private int index;
  private VortexParser<?, T> vortexParser;

  private HdfsCacheKey(){
  }

  public HdfsCacheKey(final String path, final int index, final String serializedInputSplit,
                      final VortexParser<?, T> vortexParser) {
    this.path = path;
    this.index = index;
    this.serializedInputSplit = serializedInputSplit;
    this.vortexParser = vortexParser;
  }

  public String getPath() {
    return path;
  }

  int getIndex() {
    return index;
  }

  public String getSerializedInputSplit() {
    return serializedInputSplit;
  }

  public VortexParser getParser() {
    return vortexParser;
  }

  @Override
  public String getId() {
    return path + index;
  }

  @Override
  public CacheKeyType getType() {
    return CacheKeyType.HDFS;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder()
        .append("HdfsCacheKey{path=").append(path).append(", index=").append(index).append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final HdfsCacheKey that = (HdfsCacheKey) o;

    if (index != that.index) {
      return false;
    }
    if (serializedInputSplit != null ?
        !serializedInputSplit.equals(that.serializedInputSplit) : that.serializedInputSplit != null) {
      return false;
    }
    return !(path != null ? !path.equals(that.path) : that.path != null);

  }

  @Override
  public int hashCode() {
    int result = serializedInputSplit != null ? serializedInputSplit.hashCode() : 0;
    result = 31 * result + (path != null ? path.hashCode() : 0);
    result = 31 * result + index;
    return result;
  }
}
