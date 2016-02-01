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

import com.github.fommil.netlib.BLAS;
import com.github.fommil.netlib.LAPACK;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.vortex.api.VortexFunction;
import org.netlib.util.intW;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class ALSFunction implements VortexFunction<ALSFunctionInput, ALSFunctionOutput> {

  private static final Logger LOG = Logger.getLogger(ALSFunction.class.getName());
  private static final LAPACK NETLIB_LAPACK = LAPACK.getInstance();
  private static final BLAS NETLIB_BLAS = BLAS.getInstance();

  private double lambda;
  private int numFeatures;
  private boolean printMSE;

  public ALSFunction(final int numFeatures, final double lambda, final boolean printMSE) {
    this.lambda = lambda;
    this.numFeatures = numFeatures;
    this.printMSE = printMSE;
  }

  private ALSFunction() {
  }

  private ResultVector getResultVector(final float[][] fixedMatrix,
                                       final double[] upperTriangularLeftMatrix,
                                       final IndexedVector input) {
    float sumSquaredError = 0.0f;
    final float[] vector = new float[numFeatures];
    final double[] rightSideVector = new double[numFeatures];
    final double[] tmp = new double[numFeatures];
    for (int i = 0; i < input.size(); i++) {
      final int ratingIndex = input.getRatingIndex(i);
      final float rating = input.getRating(i);
      for (int j = 0; j < numFeatures; j++) {
        tmp[j] = fixedMatrix[ratingIndex][j];
      }

      NETLIB_BLAS.dspr("U", numFeatures, 1.0, tmp, 1, upperTriangularLeftMatrix);
      if (rating != 0.0) {
        NETLIB_BLAS.daxpy(numFeatures, rating, tmp, 1, rightSideVector, 1);
      }
    }

    final double regParam = lambda * input.size();
    int a = 0;
    int b = 2;
    while (a < upperTriangularLeftMatrix.length) {
      upperTriangularLeftMatrix[a] += regParam;
      a += b;
      b += 1;
    }

    final intW info = new intW(0);
    NETLIB_LAPACK.dppsv("U", numFeatures, 1, upperTriangularLeftMatrix, rightSideVector, numFeatures, info);
    if (info.val != 0) {
      throw new RuntimeException("returned info value : " + info.val);
    }

    for (int i = 0; i < vector.length; i++) {
      vector[i] = (float)rightSideVector[i];
    }

    if (printMSE) {
      for (int i = 0; i < input.size(); i++) {
        final int ratingIndex = input.getRatingIndex(i);
        float predicted = 0.0f;
        for (int j = 0; j < numFeatures; j++) {
          predicted += fixedMatrix[ratingIndex][j] * vector[j];
        }

        final float diff = predicted - input.getRating(i);
        sumSquaredError += diff * diff;
      }
    }

    return new ResultVector(input.getIndex(), input.size(), sumSquaredError, vector);
  }

  @Override
  public ALSFunctionOutput call(final ALSFunctionInput alsFunctionInput) throws Exception {
    final Runtime r = Runtime.getRuntime();
    final long startTime = System.currentTimeMillis();
    final long startMemory = (r.totalMemory() - r.freeMemory())/1048576;

    final float[][] fixedMatrix = alsFunctionInput.getFixedMatrix();
    final long modelLoadedTime = System.currentTimeMillis();
    final long modelLoadedMemory = (r.totalMemory() - r.freeMemory())/1048576;

    final List<IndexedVector> indexedVectors = alsFunctionInput.getIndexedVectors();
    final long trainingLoadedTime = System.currentTimeMillis();
    final long trainingLoadedMemory = (r.totalMemory() - r.freeMemory())/1048576;

    LOG.log(Level.INFO, "!V!Init\t{0}\tUsed\t{1}->{2}->{3}\tMax\t{4}\tTotal\t{5}",
        new Object[] {InetAddress.getLocalHost().getHostName(),
            startMemory, modelLoadedMemory, trainingLoadedMemory, r.maxMemory()/1048576, r.totalMemory()/1048576});

    final double[] upperTriangularLeftMatrix = new double[numFeatures * (numFeatures + 1) / 2];
    final int size = indexedVectors.size();
    final ResultVector[] ret = new ResultVector[size];

    int numTotalRating = 0;
    int i = 0;
    for (final IndexedVector indexedVector : indexedVectors) {
      for (int j = 0; j < upperTriangularLeftMatrix.length; j++) {
        upperTriangularLeftMatrix[j] = 0.0;
      }

      final ResultVector resultVector = getResultVector(fixedMatrix, upperTriangularLeftMatrix, indexedVector);
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

    return new ALSFunctionOutput(ret, startTime, modelLoadedTime, trainingLoadedTime, finishTime);
  }

  @Override
  public Codec<ALSFunctionInput> getInputCodec() {
    return new ALSFunctionInputCodec();
  }

  @Override
  public Codec<ALSFunctionOutput> getOutputCodec() {
    return new ALSFunctionOutputCodec();
  }
}
