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

import org.apache.reef.io.Tuple;
import org.apache.reef.vortex.api.VortexFunction;

import java.util.List;

public final class SumFunction implements VortexFunction<SumFunctionInput, double[]> {

  private int numItems;

  private SumFunction() {
  }

  public SumFunction(final int numItems) {
    this.numItems = numItems;
  }

  private double[] getAverageVector(final List<IndexedVector> indexedVectors) {
    final double averageVector[] = new double[numItems];
    for (final IndexedVector indexedVector : indexedVectors) {
      for (final Tuple<Integer, Double> rating : indexedVector.getVector()) {
        averageVector[rating.getKey()] += rating.getValue();
      }
    }

    return averageVector;
  }

  @Override
  public double[] call(final SumFunctionInput input) throws Exception {
    return getAverageVector(input.getUserVectors());
  }
}
