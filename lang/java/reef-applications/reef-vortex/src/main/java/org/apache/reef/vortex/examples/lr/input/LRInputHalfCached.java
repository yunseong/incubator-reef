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
package org.apache.reef.vortex.examples.lr.input;

import org.apache.reef.vortex.api.VortexCacheable;
import org.apache.reef.vortex.common.CacheKey;
import org.apache.reef.vortex.common.exceptions.VortexCacheException;
import org.apache.reef.vortex.evaluator.VortexCache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Input used in Logistic Regression which caches the training data only.
 */
public class LRInputHalfCached implements Serializable, VortexCacheable {
  private CacheKey<ArrayList<ArrayBasedVector>> trainingDataKey;
  private SparseVector parameterVector;
  private int modelDim;

  public LRInputHalfCached() {
  }

  public LRInputHalfCached(final SparseVector parameterVector,
                           final CacheKey<ArrayList<ArrayBasedVector>> trainingDataKey,
                           final int modelDim) {
    this.parameterVector = parameterVector;
    this.trainingDataKey = trainingDataKey;
    this.modelDim = modelDim;
  }

  public SparseVector getParameterVector() {
    return parameterVector;
  }

  public TrainingData getTrainingData() throws VortexCacheException, ParseException {
    return DataParser.parseTrainingData(VortexCache.getData(trainingDataKey), modelDim);
  }

  @Override
  public List<CacheKey> getCachedKeys() {
    final List<CacheKey> keys = new ArrayList<>(1);
    keys.add(trainingDataKey);
    return keys;
  }
}