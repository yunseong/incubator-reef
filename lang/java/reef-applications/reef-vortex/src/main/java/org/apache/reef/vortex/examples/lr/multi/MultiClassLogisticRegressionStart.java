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
package org.apache.reef.vortex.examples.lr.multi;

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.vortex.api.FutureCallback;
import org.apache.reef.vortex.api.VortexStart;
import org.apache.reef.vortex.api.VortexThreadPool;
import org.apache.reef.vortex.common.CacheKey;
import org.apache.reef.vortex.common.HdfsCacheKey;
import org.apache.reef.vortex.common.exceptions.VortexCacheException;
import org.apache.reef.vortex.examples.lr.RowParser;
import org.apache.reef.vortex.examples.lr.vector.DenseVector;
import org.apache.reef.vortex.examples.lr.multi.input.MultiClassGradientFunctionInput;
import org.apache.reef.vortex.examples.lr.multi.output.MultiClassGradientFunctionOutput;

import javax.inject.Inject;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Logistic Regression User Code Example using URL Reputation Data Set.
 * http://archive.ics.uci.edu/ml/machine-learning-databases/url/url.names
 * The model and training data is cached
 */
final class MultiClassLogisticRegressionStart implements VortexStart {
  private static final Logger LOG = Logger.getLogger(MultiClassLogisticRegressionStart.class.getName());
  private static final double STEP_SIZE = 0.01;
  private final String path;
  private final int numIter;

  private final int divideFactor;
  private final int modelDim;
  private final int numLabels;

  @Inject
  private MultiClassLogisticRegressionStart(
      @Parameter(MultiClassLogisticRegression.DivideFactor.class) final int divideFactor,
      @Parameter(MultiClassLogisticRegression.NumIter.class) final int numIter,
      @Parameter(MultiClassLogisticRegression.Path.class) final String path,
      @Parameter(MultiClassLogisticRegression.ModelDim.class) final int modelDim,
      @Parameter(MultiClassLogisticRegression.NumLabels.class) final int numLabels) {
    this.divideFactor = divideFactor;
    this.numIter = numIter;
    this.path = path;
    this.modelDim = modelDim;
    this.numLabels = numLabels;
  }

  /**
   * Perform a simple vector multiplication on Vortex.
   */
  @Override
  public void start(final VortexThreadPool vortexThreadPool) {

    // Measure job finish time from here
    final long start = System.currentTimeMillis();

    try {
      final DenseVector[] model = new DenseVector[numLabels];
      for (int i = 0; i < numLabels; i++) {
        model[i] = new DenseVector(modelDim);
      }

      final HdfsCacheKey[] partitions = vortexThreadPool.cache(path, divideFactor, new RowParser());

      LOG.log(Level.INFO, "#V#start\tDIVIDE_FACTOR\t{0}\tSPLITS\t{1}\tNUM_ITER\t{2}",
          new Object[]{divideFactor, partitions.length, numIter});

      // For each iteration...
      for (int iteration = 1; iteration <= numIter; iteration++) {
        // Process the partial result and update to the cache
        final CacheKey<DenseVector[]> parameterKey =
            vortexThreadPool.cache("param" + iteration, model, new ModelCodec());

        // Launch tasklets, each operating on a partition
        final CountDownLatch latch = new CountDownLatch(partitions.length);
        final AccuracyMeasurer measurer = new AccuracyMeasurer();
        for (final CacheKey partition : partitions) {
          final int tIteration = iteration;
          vortexThreadPool.submit(new MultiClassGradientFunction(),
              new MultiClassGradientFunctionInput(parameterKey, partition),
              new FutureCallback<MultiClassGradientFunctionOutput>() {
                @Override
                public void onSuccess(final MultiClassGradientFunctionOutput result) {
                  processResult(model, result, tIteration, measurer);
//                  LOG.log(Level.INFO, "{0} Tasklets are remaining in this round", latch.getCount());
                  latch.countDown();
                }

                @Override
                public void onFailure(final Throwable t) {
                  throw new RuntimeException(t);
                }
              });
        }
        latch.await();
        LOG.log(Level.INFO, "@V@iteration\t{0}\taccuracy\t{1}", new Object[]{iteration, measurer.getAccuracy()});
        vortexThreadPool.invalidate(parameterKey);
      }
      final long duration = System.currentTimeMillis() - start;
      LOG.log(Level.INFO, "#V#finish\t{0}", duration);
    } catch (final InterruptedException | VortexCacheException  e) {
      final long duration = System.currentTimeMillis() - start;
      LOG.log(Level.WARNING, "#V#failed after " + duration, e);
      throw new RuntimeException("System has failed!", e);
    }
  }

  private synchronized void processResult(final DenseVector[] previousModel,
                                          final MultiClassGradientFunctionOutput result,
                                          final int tIteration, final AccuracyMeasurer measurer) {
    final float stepSize = (float) (STEP_SIZE/ Math.sqrt(tIteration));

    measurer.add(result.getCountTotal(), result.getCountPositive());

    assert previousModel.length == numLabels;
    assert result.getPartialGradient().length == numLabels;

    // SimpleUpdater. No regularization
    for (int i = 0; i < numLabels; i++) {
      previousModel[i].axpy(-stepSize, result.getPartialGradient()[i]);
    }
  }

  static class AccuracyMeasurer {
    private int countTotal;
    private int countPositive;

    AccuracyMeasurer() {
      this.countTotal = 0;
      this.countPositive = 0;
    }

    void add(final int numTotal, final int numPositive) {
      this.countTotal += numTotal;
      this.countPositive += numPositive;
    }

    double getAccuracy() {
      return (double) countPositive / countTotal;
    }
  }
}