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

import org.apache.commons.math3.linear.*;
import org.apache.commons.math3.linear.CholeskyDecomposition;
import org.apache.reef.io.Tuple;
import org.apache.reef.vortex.api.VortexFunction;

import java.io.Serializable;
import java.util.List;
import java.util.logging.Logger;

public final class ALSFunction implements Serializable, VortexFunction<ALSFunctionInput, ResultVector[]> {

  private final static Logger LOG = Logger.getLogger(ALSFunction.class.getName());

  private double lambda;
  private int numFeatures;

  public ALSFunction(final int numFeatures, final double lambda) {
    this.lambda = lambda;
    this.numFeatures = numFeatures;
  }

  private ALSFunction() {
  }

  private ResultVector getResultVector(final RealMatrix fixedMatrix,
      final int index, final List<Tuple<Integer, Double>> sparseRatingVector) {

    final double[] vector = new double[numFeatures];

    final int numRatings = sparseRatingVector.size();
    final int[] ratingIndexArr = new int[numRatings];
    final RealVector ratingVector = new ArrayRealVector(numRatings);
    // final Vector ratingVector = new DenseVector(numRatings);

    int i = 0;
    int beforeIndex = -1;
    for (final Tuple<Integer, Double> tuple : sparseRatingVector) {
      if (beforeIndex > tuple.getKey()) {
        throw new RuntimeException();
      }
      ratingIndexArr[i] = tuple.getKey();
      ratingVector.setEntry(i, tuple.getValue());
      i++;
    }

    // compute matrix M from feature vectors
    //final Matrix userItemMatrix = new DenseMatrix(numFeatures, numRatings);
    final RealMatrix userItemMatrix = new Array2DRowRealMatrix(numFeatures, numRatings);

    i = 0;
    for (final int ratingIndex : ratingIndexArr) {
      userItemMatrix.setColumn(i++, fixedMatrix.getColumn(ratingIndex));
    }

    final double[] diagonalElements = new double[numFeatures];
    for (int j = 0; j < diagonalElements.length; j++) {
      diagonalElements[j] = lambda * numRatings;
    }

    // lambda * numRatings * E
    final RealMatrix regularizationMatrix = new DiagonalMatrix(diagonalElements);

    // M * M' + lambda * numRatings * E
    final RealMatrix leftSideMatrix = userItemMatrix.multiply(userItemMatrix.transpose()).add(regularizationMatrix);

    // R(i) = ratingVector

    // M * R(i)
    final RealVector rightSideVector = userItemMatrix.operate(ratingVector);

    final DecompositionSolver solver = new CholeskyDecomposition(leftSideMatrix).getSolver();

    // (M * M' + lambda * numRatings * E)^(-1) * M * R(i)
    final RealVector newVec = solver.solve(rightSideVector);

    for (i = 0; i < numFeatures; i++) {
      vector[i] = newVec.getEntry(i);
    }

    double error = 0.0;
    for (final Tuple<Integer, Double> tuple : sparseRatingVector) {
      double diff = newVec.dotProduct(fixedMatrix.getColumnVector(tuple.getKey())) - tuple.getValue();
      error += diff * diff;
    }

    return new ResultVector(index, sparseRatingVector.size(), error, vector);
  }

  @Override
  public ResultVector[] call(final ALSFunctionInput alsFunctionInput) throws Exception {
    final RealMatrix fixedMatrix = alsFunctionInput.getFixedMatrix();
    final List<IndexedVector> indexedVectors = alsFunctionInput.getIndexedVectors();
    final int size = indexedVectors.size();
    final ResultVector[] ret = new ResultVector[size];

    int i = 0;
    for (final IndexedVector indexedVector : indexedVectors) {
      ret[i++] = getResultVector(fixedMatrix, indexedVector.getIndex(), indexedVector.getVector());
    }

    return ret;
  }
}
