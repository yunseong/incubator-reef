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

import com.github.fommil.netlib.LAPACK;
import org.apache.commons.math3.linear.*;
import org.apache.reef.io.Tuple;
import org.apache.reef.vortex.api.VortexFunction;
import org.netlib.util.intW;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class ALSFunction implements Serializable, VortexFunction<ALSFunctionInput, ResultVector[]> {

  private final static Logger LOG = Logger.getLogger(ALSFunction.class.getName());
  private final static LAPACK NETLIB_LAPACK = LAPACK.getInstance();

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

    // (M * M' + lambda * numRatings * E)^(-1) * M * R(i)
    final double[] newVector = solve(leftSideMatrix, rightSideVector, numFeatures);

    double error = 0.0;
    for (final Tuple<Integer, Double> tuple : sparseRatingVector) {
      double diff = dot((fixedMatrix.getColumnVector(tuple.getKey())), newVector) - tuple.getValue();
      error += diff * diff;
    }

    return new ResultVector(index, sparseRatingVector.size(), error, newVector);
  }

  private double dot(final RealVector v1, final double[] v2) {
    double acc = 0.0;
    for (int i = 0; i < v2.length; i++) {
      acc += v1.getEntry(i) * v2[i];
    }

    return acc;
  }

  private double[] solve(final RealMatrix leftSideMatrix, final RealVector rightSideVector, final int numFeatures) {
    final double[] upperTriangularLeftMatrix = new double[numFeatures * (numFeatures + 1) / 2];
    int index = 0;
    for (int c = 0; c < numFeatures; c++) {
      for (int r = 0; r <= c; r++) {
        upperTriangularLeftMatrix[index++] = leftSideMatrix.getEntry(r, c);
      }
    }

    final double[] rightDoubleVector = new double[numFeatures];
    for (int i = 0; i < numFeatures; i++) {
      rightDoubleVector[i] = rightSideVector.getEntry(i);
    }

    final intW info = new intW(0);
    NETLIB_LAPACK.dppsv("U", numFeatures, 1, upperTriangularLeftMatrix, rightDoubleVector, numFeatures, info);
    if (info.val != 0) {
      throw new RuntimeException("returned info value : " + info.val);
    }

    return rightDoubleVector;
  }

  @Override
  public ResultVector[] call(final ALSFunctionInput alsFunctionInput) throws Exception {
    final Runtime r = Runtime.getRuntime();
    final long startTime = System.currentTimeMillis();
    final long startMemory = (r.totalMemory() - r.freeMemory())/1048576;

    final RealMatrix fixedMatrix = alsFunctionInput.getFixedMatrix();
    final long modelLoadedTime = System.currentTimeMillis();
    final long modelLoadedMemory = (r.totalMemory() - r.freeMemory())/1048576;

    final List<IndexedVector> indexedVectors = alsFunctionInput.getIndexedVectors();
    final long trainingLoadedTime = System.currentTimeMillis();
    final long trainingLoadedMemory = (r.totalMemory() - r.freeMemory())/1048576;

    LOG.log(Level.INFO, "!V!Init\t{0}\tUsed\t{1}->{2}->{3}\tMax\t{4}\tTotal\t{5}",
        new Object[] {InetAddress.getLocalHost().getHostName(),
            startMemory, modelLoadedMemory, trainingLoadedMemory, r.maxMemory()/1048576, r.totalMemory()/1048576});

    final int size = indexedVectors.size();
    final ResultVector[] ret = new ResultVector[size];

    int numTotalRating = 0;
    int i = 0;
    for (final IndexedVector indexedVector : indexedVectors) {
      final ResultVector resultVector = getResultVector(
          fixedMatrix, indexedVector.getIndex(), indexedVector.getVector());
      numTotalRating += resultVector.getNumRatings();
      ret[i++] = resultVector;
    }

    final long finishTime = System.currentTimeMillis();
    final long executionTime = finishTime - trainingLoadedTime;
    final long modelOverhead = modelLoadedTime - startTime;
    final long trainingOverhead = trainingLoadedTime - modelLoadedTime;

    LOG.log(Level.INFO, "!V!\t{0}\tUsed\t{1}->{2}\tMax\t{3}\tTotal\t{4}" +
            "\tExecution\t{5}\tModel\t{6}\tTraining\t{7}\tRatingNum\t{8}\tkey\t{9}",
        new Object[]{
            InetAddress.getLocalHost().getHostName(), startMemory, (r.totalMemory() - r.freeMemory())/1048576,
            r.maxMemory()/1048576, r.totalMemory()/1048576,
            executionTime, modelOverhead, trainingOverhead, numTotalRating,
            Arrays.toString(alsFunctionInput.getCachedKeys().toArray())});

    return ret;
  }
}
