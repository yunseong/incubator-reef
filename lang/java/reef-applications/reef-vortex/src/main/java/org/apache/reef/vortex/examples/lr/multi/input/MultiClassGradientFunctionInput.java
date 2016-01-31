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
package org.apache.reef.vortex.examples.lr.multi.input;

import org.apache.reef.vortex.api.VortexCacheable;
import org.apache.reef.vortex.common.CacheKey;
import org.apache.reef.vortex.common.exceptions.VortexCacheException;
import org.apache.reef.vortex.evaluator.VortexCache;
import org.apache.reef.vortex.examples.lr.Row;
import org.apache.reef.vortex.examples.lr.vector.DenseVector;

import java.util.ArrayList;
import java.util.List;

/**
 * Input used in Logistic Regression which caches the training data only.
 */
public final class MultiClassGradientFunctionInput implements VortexCacheable {
  private static final Object LOCK = new Object();
  private static CacheKey PREV_PARAMETER_VECTOR_KEY;

  private CacheKey<ArrayList<Row>> trainingDataKey;
  private CacheKey<DenseVector[]> parameterVectorKey;

  private MultiClassGradientFunctionInput() {
  }

  public MultiClassGradientFunctionInput(final CacheKey<DenseVector[]> parameterVectorKey,
                                         final CacheKey<ArrayList<Row>> trainingDataKey) {
    this.parameterVectorKey = parameterVectorKey;
    this.trainingDataKey = trainingDataKey;
  }

  public DenseVector[] getParameterVector() throws VortexCacheException {
    synchronized (LOCK) {
      if (PREV_PARAMETER_VECTOR_KEY != null &&
          !PREV_PARAMETER_VECTOR_KEY.getId().equals(parameterVectorKey.getId())) {
        VortexCache.invalidate(PREV_PARAMETER_VECTOR_KEY);
        PREV_PARAMETER_VECTOR_KEY = parameterVectorKey;
      }
      return VortexCache.getData(parameterVectorKey);
    }
  }

  public ArrayList<Row> getTrainingData() throws VortexCacheException {
    return VortexCache.getData(trainingDataKey);
  }

  @Override
  public List<CacheKey> getCachedKeys() {
    final List<CacheKey> keys = new ArrayList<>();
    keys.add(trainingDataKey);
    return keys;
  }
}