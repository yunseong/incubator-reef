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
import org.apache.reef.vortex.common.MasterCacheKey;
import org.apache.reef.vortex.evaluator.VortexCache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public final class ALSFunctionInput implements Serializable, VortexCacheable {

  private HDFSBackedCacheKey<List<IndexedVector>> vectorDataKey;
  private MasterCacheKey<float[][]> fixedMatrixKey;
  private List<CacheKey> keys;

  private ALSFunctionInput() {
  }

  public ALSFunctionInput(
      final HDFSBackedCacheKey<List<IndexedVector>> vectorDataKey,
      final MasterCacheKey<float[][]> fixedMatrixKey) {
    this.vectorDataKey = vectorDataKey;
    this.fixedMatrixKey = fixedMatrixKey;

    this.keys = new ArrayList<>(2);
    this.keys.add(vectorDataKey);
    this.keys.add(fixedMatrixKey);
  }

  public synchronized List<IndexedVector> getIndexedVectors() throws Exception {
    return VortexCache.getData(vectorDataKey);
  }

  public synchronized float[][] getFixedMatrix() throws Exception {
    return VortexCache.getData(fixedMatrixKey);
  }

  @Override
  public List<CacheKey> getCachedKeys() {
    return keys;
  }
}
