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
package org.apache.reef.vortex.examples.matmul;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.vortex.api.VortexFunction;

import java.util.ArrayList;
import java.util.Vector;

/**
 * Outputs multiplication of two vectors.
 */
final class MatMulFunction
    implements VortexFunction<Pair<ArrayList<Vector<Double>>, ArrayList<Vector<Double>>>, ArrayList<Vector<Double>>> {
  /**
   * Outputs multiplication of two vectors.
   */
  @Override
  public ArrayList<Vector<Double>> call(final Pair<ArrayList<Vector<Double>>, ArrayList<Vector<Double>>> matrixPair)
      throws Exception {
    final ArrayList<Vector<Double>> leftMatrix = matrixPair.getLeft();
    final ArrayList<Vector<Double>> rightMatrix = matrixPair.getRight();
    final ArrayList<Vector<Double>> resultMatrix = new ArrayList<>();

    for (final Vector<Double> leftVector : leftMatrix) {
      final Vector<Double> resultVector = new Vector<>();
      for (final Vector<Double> rightVector : rightMatrix) {
        assert(leftVector.size() == rightVector.size());
        assert(leftVector.size() > 0);

        final int vectorSize = leftVector.size();
        double result = 0;
        for (int k = 0; k < vectorSize; k++) {
          result += leftVector.get(k) * rightVector.get(k);
        }
        resultVector.add(result);
      }
      resultMatrix.add(resultVector);
    }

    return resultMatrix;
  }
}
