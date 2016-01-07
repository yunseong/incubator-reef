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

package org.apache.reef.vortex.examples.als;

import org.apache.reef.vortex.api.VortexCacheable;
import org.apache.reef.vortex.common.CacheKey;
import org.apache.reef.vortex.common.HDFSBackedCacheKey;
import org.apache.reef.vortex.common.exceptions.VortexCacheException;
import org.apache.reef.vortex.evaluator.VortexCache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public final class SumFunctionInput implements Serializable, VortexCacheable {

  private HDFSBackedCacheKey<List<IndexedVector>> userVectorDataKey;
  private List<CacheKey> keys;

  private SumFunctionInput() {
  }

  public SumFunctionInput(final HDFSBackedCacheKey<List<IndexedVector>> userVectorDataKey) {
    this.userVectorDataKey = userVectorDataKey;
    this.keys = new ArrayList<>();
    this.keys.add(userVectorDataKey);
  }

  public List<IndexedVector> getUserVectors() throws VortexCacheException {
    return VortexCache.getData(userVectorDataKey);
  }

  @Override
  public List<CacheKey> getCachedKeys() {
    return keys;
  }
}
