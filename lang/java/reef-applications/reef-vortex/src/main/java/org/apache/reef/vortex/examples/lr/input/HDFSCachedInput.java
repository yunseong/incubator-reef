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
import org.apache.reef.vortex.common.MasterCacheKey;
import org.apache.reef.vortex.common.HDFSBackedCacheKey;
import org.apache.reef.vortex.common.exceptions.VortexCacheException;
import org.apache.reef.vortex.evaluator.VortexCache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Input used in Logistic Regression which caches the training data only.
 */
public final class HDFSCachedInput implements Serializable, VortexCacheable {
  private HDFSBackedCacheKey trainingDataKey;
  private MasterCacheKey<SparseVector> parameterVectorKey;

  private HDFSCachedInput() {
  }

  public HDFSCachedInput(final MasterCacheKey<SparseVector> parameterVectorKey,
                         final HDFSBackedCacheKey trainingDataKey) {
    this.parameterVectorKey = parameterVectorKey;
    this.trainingDataKey = trainingDataKey;
  }

  public SparseVector getParameterVector() throws VortexCacheException {
    return VortexCache.getData(parameterVectorKey);
  }

  public ArrayList<ArrayBasedVector> getTrainingData() throws VortexCacheException, ParseException {
    final List<String> texts = VortexCache.getData(trainingDataKey);
    final ArrayList<ArrayBasedVector> result = new ArrayList<>(texts.size());
    for (final String text : texts) {
      result.add(parseLine(text));
    }
    return result;
  }

  @Override
  public List<CacheKey> getCachedKeys() {
    final List<CacheKey> keys = new ArrayList<>();
    keys.add(trainingDataKey);
    keys.add(parameterVectorKey);
    return keys;
  }

  /**
   * Parse a line and create a training data.
   */
  private static ArrayBasedVector parseLine(final String line) throws ParseException {
    final String[] split = line.split(" ");

    try {
      final int output = Integer.valueOf(split[0]);
      final int[] indices = new int[split.length - 1];
      final float[] values = new float[split.length - 1];

      for (int i = 1; i < split.length; i++) {
        final String[] column = split[i].split(":");

        final int index = Integer.valueOf(column[0]);
        final float value = Float.valueOf(column[1]);

        indices[i-1] = index;
        values[i-1] = value;
      }
      return new ArrayBasedVector(values, indices, output);

    } catch (final NumberFormatException e) {
      throw new ParseException(e.getMessage());
    }
  }
}
