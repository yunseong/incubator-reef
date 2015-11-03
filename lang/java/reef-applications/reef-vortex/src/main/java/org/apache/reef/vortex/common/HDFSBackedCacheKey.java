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
public final class HDFSBackedCacheKey implements Serializable {
  private String serializedJobConf;
  private String serializedInputSplit;
  private String path;

  private HDFSBackedCacheKey(){
  }

  public HDFSBackedCacheKey(final String path, final String serializedJobConf, final String serializedInputSplit) {
    this.path = path;
    this.serializedJobConf = serializedJobConf;
    this.serializedInputSplit = serializedInputSplit;
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
}
