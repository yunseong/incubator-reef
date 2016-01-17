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

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.vortex.api.FutureCallback;
import org.apache.reef.vortex.api.VortexStart;
import org.apache.reef.vortex.api.VortexThreadPool;
import org.apache.reef.vortex.common.CacheKey;
import org.apache.reef.vortex.common.HdfsCacheKey;
import org.apache.reef.vortex.examples.lr.input.GradientFunctionInput;
import org.apache.reef.vortex.examples.lr.output.GradientFunctionOutput;
import org.apache.reef.vortex.examples.lr.vector.DenseVector;
import org.apache.reef.vortex.examples.lr.vector.DenseVectorCodec;

import javax.inject.Inject;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Logistic Regression User Code Example using URL Reputation Data Set.
 * http://archive.ics.uci.edu/ml/machine-learning-databases/url/url.names
 * The model and training data is cached
 */
final class LogisticRegressionStart implements VortexStart {
  private static final Logger LOG = Logger.getLogger(LogisticRegressionStart.class.getName());
  private static final double STEP_SIZE = 0.01;
  private final String path;
  private final int numIter;

  private final int divideFactor;
  private final int modelDim;

  @Inject
  private LogisticRegressionStart(@Parameter(LogisticRegression.DivideFactor.class) final int divideFactor,
                                  @Parameter(LogisticRegression.NumIter.class) final int numIter,
                                  @Parameter(LogisticRegression.Path.class) final String path,
                                  @Parameter(LogisticRegression.ModelDim.class) final int modelDim) {
    this.divideFactor = divideFactor;
    this.numIter = numIter;
    this.path = path;
    this.modelDim = modelDim;
  }

  /**
   * Perform a simple vector multiplication on Vortex.
   */
  @Override
  public void start(final VortexThreadPool vortexThreadPool) {

    // Measure job finish time from here
    final long start = System.currentTimeMillis();

    try {
      final DenseVector model = new DenseVector(modelDim);
      final HdfsCacheKey[] partitions = vortexThreadPool.cache(path, divideFactor, new RowParser());

      LOG.log(Level.INFO, "#V#start\tDIVIDE_FACTOR\t{0}\tSPLITS\t{1}\tNUM_ITER\t{2}",
          new Object[]{divideFactor, partitions.length, numIter});

      // For each iteration...
      for (int iteration = 1; iteration <= numIter; iteration++) {
        // Process the partial result and update to the cache
        final CacheKey<DenseVector> parameterKey =
            vortexThreadPool.cache("param" + iteration, model, new DenseVectorCodec());

        // Launch tasklets, each operating on a partition
        final CountDownLatch latch = new CountDownLatch(partitions.length);
        final AccuracyMeasurer measurer = new AccuracyMeasurer();
        for (final CacheKey partition : partitions) {
          final int tIteration = iteration;
          vortexThreadPool.submit(new GradientFunction(),
              new GradientFunctionInput(parameterKey, partition),
              new FutureCallback<GradientFunctionOutput>() {
                @Override
                public void onSuccess(final GradientFunctionOutput result) {
                  processResult(model, result, tIteration, measurer);
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
      }
      final long duration = System.currentTimeMillis() - start;
      LOG.log(Level.INFO, "#V#finish\t{0}", duration);
    } catch (final Exception e) {
      final long duration = System.currentTimeMillis() - start;
      LOG.log(Level.WARNING, "#V#failed after " + duration, e);
    }
  }

  private synchronized void processResult(final DenseVector previousModel, final GradientFunctionOutput result,
                                          final int tIteration, final AccuracyMeasurer measurer) {
    final float stepSize = (float) (STEP_SIZE/ Math.sqrt(tIteration));

    measurer.add(result.getCountTotal(), result.getCountPositive());

    // SimpleUpdater. No regularization
    previousModel.axpy(-stepSize, result.getPartialGradient());
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