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
package org.apache.reef.vortex.examples.lr;

import org.apache.reef.vortex.common.CacheKey;
import org.apache.reef.vortex.common.exceptions.VortexCacheException;
import org.apache.reef.vortex.evaluator.VortexCache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

/**
 * Created by v-yunlee on 9/23/2015.
 */
public class LRInputHalfCached implements Serializable {
  private CacheKey<ArrayList<Vector<Double>>> trainingDataKey;
  private Vector<Double> parameterVector;
  public LRInputHalfCached(final Vector<Double> parameterVector,
                           final CacheKey<ArrayList<Vector<Double>>> trainingDataKey) {
    this.parameterVector = parameterVector;
    this.trainingDataKey = trainingDataKey;
  }

  public Vector<Double> getParameterVector() {
    return parameterVector;
  }

  public ArrayList<Vector<Double>> getTrainingData() throws VortexCacheException {
    System.out.println("getTrainingData: " + trainingDataKey);
    return VortexCache.getData(trainingDataKey);
  }
}
