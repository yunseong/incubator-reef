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
 * Key used to get the data from the Worker cache
 * Users should assign unique name to distinguish the keys.
 */
public final class HDFSBackedCacheKey<T extends Serializable> implements CacheKey<T> {
  private String serializedJobConf;
  private String serializedInputSplit;
  private String path;
  private int index;
  private VortexParser vortexParser;

  private HDFSBackedCacheKey(){
  }

  public HDFSBackedCacheKey(final String path, final int index, final String serializedJobConf,
                            final String serializedInputSplit, final VortexParser vortexParser) {
    this.path = path;
    this.index = index;
    this.serializedJobConf = serializedJobConf;
    this.serializedInputSplit = serializedInputSplit;
    this.vortexParser = vortexParser;
  }

  public String getPath() {
    return path;
  }

  public String getSerializedJobConf() {
    return serializedJobConf;
  }

  public String getSerializedInputSplit() {
    return serializedInputSplit;
  }

  public VortexParser getParser() {
    return vortexParser;
  }

  @Override
  public String toString() {
    return "HDFSBackedCacheKey{" +
        "path=" + path +
        ", index='" + index + '\'' +
        '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final HDFSBackedCacheKey that = (HDFSBackedCacheKey) o;

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

  @Override
  public String getId() {
    return path+index;
  }

  @Override
  public CacheKeyType getType() {
    return CacheKeyType.HDFS;
  }
}
