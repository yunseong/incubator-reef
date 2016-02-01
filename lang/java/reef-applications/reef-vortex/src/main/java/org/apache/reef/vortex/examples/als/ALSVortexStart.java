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

import org.apache.commons.io.FileUtils;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.vortex.api.FutureCallback;
import org.apache.reef.vortex.api.VortexFuture;
import org.apache.reef.vortex.api.VortexStart;
import org.apache.reef.vortex.api.VortexThreadPool;
import org.apache.reef.vortex.common.CacheKey;
import org.apache.reef.vortex.common.HdfsCacheKey;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by kgw on 2016. 1. 7..
 */
public final class ALSVortexStart implements VortexStart {

  private static final Logger LOG = Logger.getLogger(ALSVortexStart.class.getName());

  private final int divideFactor;
  private final int numIter;
  private final double lambda;
  private final String userDataMatrixPath;
  private final String itemDataMatrixPath;
  private final int numUsers;
  private final int numItems;
  private final int numFeatures;

  private final float[][] userMatrix;
  private final float[][] itemMatrix;

  private final boolean isSaveModel;
  private final boolean printMSE;

  @Inject
  private ALSVortexStart(
      @Parameter(AlternatingLeastSquares.DivideFactor.class) final int divideFactor,
      @Parameter(AlternatingLeastSquares.NumIter.class) final int numIter,
      @Parameter(AlternatingLeastSquares.Lambda.class) final double lambda,
      @Parameter(AlternatingLeastSquares.UserDataMatrixPath.class) final String userDataMatrixPath,
      @Parameter(AlternatingLeastSquares.ItemDataMatrixPath.class) final String itemDataMatrixPath,
      @Parameter(AlternatingLeastSquares.NumUsers.class) final int numUsers,
      @Parameter(AlternatingLeastSquares.NumItems.class) final int numItems,
      @Parameter(AlternatingLeastSquares.NumFeatures.class) final int numFeatures,
      @Parameter(AlternatingLeastSquares.SaveModel.class) final boolean isSaveModel,
      @Parameter(AlternatingLeastSquares.PrintMSE.class) final boolean printMSE) {
    this.divideFactor = divideFactor;
    this.numIter = numIter;
    this.lambda = lambda;
    this.userDataMatrixPath = userDataMatrixPath;
    this.itemDataMatrixPath = itemDataMatrixPath;
    this.numUsers = numUsers;
    this.numItems = numItems;
    this.numFeatures = numFeatures;
    this.isSaveModel = isSaveModel;
    this.printMSE = printMSE;

    this.userMatrix = new float[numUsers][numFeatures];
    this.itemMatrix = new float[numItems][numFeatures];
  }

  @Override
  public void start(final VortexThreadPool vortexThreadPool) {
    // Measure job finish time from here
    final long start = System.currentTimeMillis();

    try {
      final HdfsCacheKey[] userDataMatrixPartition = vortexThreadPool.cache(userDataMatrixPath, divideFactor,
          new DataParser());
      final HdfsCacheKey[] itemDataMatrixPartition = vortexThreadPool.cache(itemDataMatrixPath, divideFactor,
          new DataParser());

      LOG.log(Level.INFO,
          "#V#startCached\tDIVIDE_FACTOR\t{0}\t\tNUM_ITER\t{1}\tUM_SPLITS\t{2}\tIM_SPLITS\t{3}",
          new Object[]{divideFactor, numIter, userDataMatrixPartition.length, itemDataMatrixPartition.length});

      initializeItemMatrix(vortexThreadPool, userDataMatrixPartition);
      final Map<String, Long> submittedTime = new HashMap<>();

      for (int iter = 0; iter < 2 * numIter; iter++) {
        if (isSaveModel) {
          saveModels("" + iter);
        }

        final double memoryIterationStarted = getRemainingMemory();

        final boolean isUpdateUserMatrix = iter % 2 == 0;

        final float[][] updatedMatrix;
        final CacheKey<float[][]> fixedMatrixKey;
        final CacheKey[] dataMatrixPartition;

        if (isUpdateUserMatrix) { // Fix ItemMatrix, solve UserMatrix
          updatedMatrix = userMatrix;
          fixedMatrixKey = vortexThreadPool.cache("item_matrix_" + iter, itemMatrix,
              new MatrixCodec());
          dataMatrixPartition = userDataMatrixPartition;
        } else { // Fix UserMatrix, solve ItemMatrix
          updatedMatrix = itemMatrix;
          fixedMatrixKey = vortexThreadPool.cache("user_matrix_" + iter, userMatrix,
              new MatrixCodec());
          dataMatrixPartition = itemDataMatrixPartition;
        }

        final double memoryAfterCached = getRemainingMemory();
        final CountDownLatch latch = new CountDownLatch(dataMatrixPartition.length);
        final BlockingDeque<ResultVector[]> resultVectors = new LinkedBlockingDeque<>();
        for (int i = 0; i < dataMatrixPartition.length; i++) {
          final String taskletId = "iter_" + iter + "_" + i;
          submittedTime.put(taskletId, System.currentTimeMillis());
          vortexThreadPool.submit(
              new ALSFunction(numFeatures, lambda, printMSE),
              new ALSFunctionInput(dataMatrixPartition[i], fixedMatrixKey, taskletId),
              new FutureCallback<ALSFunctionOutput>() {

                @Override
                public void onSuccess(final ALSFunctionOutput result) {
                  logTaskletLifeTimes(taskletId, submittedTime.get(taskletId), result.launched, result.modelLoaded,
                      result.trainingLoaded, result.computeFinished, System.currentTimeMillis());
                  resultVectors.add(result.resultVectors);
                  latch.countDown();
                }

                @Override
                public void onFailure(final Throwable t) {
                  throw new RuntimeException(t);
                }
              }
          );
        }

        latch.await();

        float sumSquareErrors = 0.0f;
        int numRatings = 0;

        for (final ResultVector[] updatedVectors : resultVectors) {
          for (final ResultVector updatedVector : updatedVectors) {
            final int index = updatedVector.getIndex();
            numRatings += updatedVector.getNumRatings();
            sumSquareErrors += updatedVector.getSumSquaredError();
            final float[] vector = updatedVector.getVector();
            for (int i = 0; i < vector.length; i++) {
              updatedMatrix[index][i] = vector[i];
            }
          }
        }

        vortexThreadPool.invalidate(fixedMatrixKey);

        final String matrixType;
        if (isUpdateUserMatrix) {
          matrixType = "User Matrix";
        } else {
          matrixType = "Item Matrix";
        }

        final float mse = sumSquareErrors / numRatings;
        final double memoryIterationEnded = getRemainingMemory();

        LOG.log(Level.INFO, "@V@iteration:{0} (update {1})\t# ratings\t{2}\tmse\t{3}\tHost\t{4}\tUsed" +
            "\t{5}->{6}->{7}\tMax\t{8}\tTotal\t{9}",
            new Object[]{
                iter, matrixType, numRatings, String.format("%.4e", mse),
                InetAddress.getLocalHost().getHostName(), memoryIterationStarted, memoryAfterCached,
                memoryIterationEnded, getMaxMemory(), getTotalMemory()});
      }

      final long duration = System.currentTimeMillis() - start;
      LOG.log(Level.INFO, "#V#finish\t{0}", duration);
    } catch (final Exception e) {
      final long duration = System.currentTimeMillis() - start;
      LOG.log(Level.WARNING, "#V#failed after " + duration, e);
    }

    if (isSaveModel) {
      saveModels("final");
    }
  }

  private void logTaskletLifeTimes(final String id,
                                   final long submitted,
                                   final long launched,
                                   final long modelLoaded,
                                   final long trainingLoaded,
                                   final long finished,
                                   final long ended) {
    LOG.log(Level.INFO, "@@Q@@!{0}!{1}!{2}!{3}!{4}!{5}!", new Object[] {
        id, launched - submitted, modelLoaded - launched, trainingLoaded - modelLoaded,
        finished - trainingLoaded, ended - finished});
  }

  private void saveModels(final String postFix) {
    final StringBuilder umSb = new StringBuilder();
    for (int i = 0; i < userMatrix.length; i++) {
      for (int j = 0; j < userMatrix[i].length; j++) {
        umSb.append(String.format("%.4f ", userMatrix[i][j]));
      }
      umSb.append("\n");
    }

    final StringBuilder imSb = new StringBuilder();
    for (int i = 0; i < itemMatrix.length; i++) {
      for (int j = 0; j < itemMatrix[i].length; j++) {
        imSb.append(String.format("%.4f ", itemMatrix[i][j]));
      }
      imSb.append("\n");
    }

    try {
      FileUtils.writeStringToFile(new File("user_feature_matrix_" + postFix + ".txt"), umSb.toString());
      FileUtils.writeStringToFile(new File("item_feature_matrix_" + postFix + ".txt"), imSb.toString());
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static final int ONE_MEGABYTE = 1024 * 1024;
  private static final Runtime RUNTIME = Runtime.getRuntime();

  private double getRemainingMemory() {
    return (RUNTIME.totalMemory() - RUNTIME.freeMemory()) / (double)ONE_MEGABYTE;
  }

  private double getMaxMemory() {
    return RUNTIME.maxMemory() / (double)ONE_MEGABYTE;
  }

  private double getTotalMemory() {
    return RUNTIME.totalMemory() / (double)ONE_MEGABYTE;
  }

  private void initializeItemMatrix(
      final VortexThreadPool vortexThreadPool,
      final HdfsCacheKey[] userDataMatriPartition) throws Exception {
    final int taskletSize = userDataMatriPartition.length;
    LOG.log(Level.INFO, "{0}", taskletSize);
    final List<VortexFuture<float[]>> futures = new ArrayList<>();

    final float[] itemRatingSum = new float[numItems];

    for (int i = 0; i < taskletSize; i++) {
      futures.add(vortexThreadPool.submit(
          new SumFunction(numItems),
          new SumFunctionInput(userDataMatriPartition[i])));
    }

    for (final VortexFuture<float[]> future : futures) {
      final float[] result = future.get();

      for (int i = 0; i < numItems; i++) {
        itemRatingSum[i] += result[i];
      }
    }

    float totalAverage = 0.0f;

    for (int i = 0; i < numItems; i++) {
      itemMatrix[i][0] = itemRatingSum[i] / numUsers;
      totalAverage += itemMatrix[i][0];
    }

    totalAverage /= numItems;
    final float maxRandomValue = totalAverage / 1000;
    for (int i = 0; i < numItems; i++) {
      for (int j = 1; j < numFeatures; j++) {
        itemMatrix[i][j] = maxRandomValue * (float)Math.random();
      }
    }
  }
}
