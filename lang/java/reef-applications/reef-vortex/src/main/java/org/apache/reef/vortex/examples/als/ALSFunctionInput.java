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
import org.apache.reef.vortex.evaluator.VortexCache;

import java.util.ArrayList;
import java.util.List;

public final class ALSFunctionInput implements VortexCacheable {

  private static final Object LOCK = new Object();
  private static CacheKey PREV_FIXED_MATRIX_CACHE_KEY;

  private CacheKey<List<IndexedVector>> vectorDataKey;
  private CacheKey<float[][]> fixedMatrixKey;
  private List<CacheKey> keys;

  private ALSFunctionInput() {
  }

  public ALSFunctionInput(
      final CacheKey<List<IndexedVector>> vectorDataKey,
      final CacheKey<float[][]> fixedMatrixKey) {
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
    synchronized (LOCK) {
      if (PREV_FIXED_MATRIX_CACHE_KEY != null &&
          !PREV_FIXED_MATRIX_CACHE_KEY.getId().equals(fixedMatrixKey.getId())) {
        VortexCache.invalidate(PREV_FIXED_MATRIX_CACHE_KEY);
        PREV_FIXED_MATRIX_CACHE_KEY = fixedMatrixKey;
      }
    }
    return VortexCache.getData(fixedMatrixKey);
  }

  @Override
  public List<CacheKey> getCachedKeys() {
    return keys;
  }
}
