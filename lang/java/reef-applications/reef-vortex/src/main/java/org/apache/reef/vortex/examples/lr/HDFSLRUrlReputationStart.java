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
import org.apache.reef.vortex.api.VortexFuture;
import org.apache.reef.vortex.api.VortexStart;
import org.apache.reef.vortex.api.VortexThreadPool;
import org.apache.reef.vortex.common.MasterCacheKey;
import org.apache.reef.vortex.common.HDFSBackedCacheKey;
import org.apache.reef.vortex.examples.lr.input.HDFSCachedInput;
import org.apache.reef.vortex.examples.lr.input.RowParser;
import org.apache.reef.vortex.examples.lr.input.DenseVector;
import org.apache.reef.vortex.failure.parameters.Delay;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Logistic Regression User Code Example using URL Reputation Data Set.
 * http://archive.ics.uci.edu/ml/machine-learning-databases/url/url.names
 * The model and training data is cached
 */
final class HDFSLRUrlReputationStart implements VortexStart {
  private static final Logger LOG = Logger.getLogger(HDFSLRUrlReputationStart.class.getName());

  private final String path;
  private final int numIter;

  private final int divideFactor;
  private final int modelDim;
  private final int numRecords;

  // For printing purpose actually.
  private final int delay;

  @Inject
  private HDFSLRUrlReputationStart(@Parameter(LogisticRegression.DivideFactor.class) final int divideFactor,
                                   @Parameter(LogisticRegression.NumIter.class) final int numIter,
                                   @Parameter(LogisticRegression.Path.class) final String path,
                                   @Parameter(LogisticRegression.ModelDim.class) final int modelDim,
                                   @Parameter(LogisticRegression.NumRecords.class) final int numRecords,
                                   @Parameter(Delay.class) final int delay) {
    this.divideFactor = divideFactor;
    this.numIter = numIter;
    this.path = path;
    this.modelDim = modelDim;
    this.numRecords = numRecords;
    this.delay = delay;
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

      final HDFSBackedCacheKey[] partitions = vortexThreadPool.cache(path, divideFactor,
          new RowParser());
      final List<VortexFuture<PartialResult>> futures = new ArrayList<>(partitions.length);

      LOG.log(Level.INFO,
          "#V#startCached\tDIVIDE_FACTOR\t{0}\tDELAY\t{1}\tNUM_ITER\t{2}\tSPLITS\t{4}",
          new Object[]{divideFactor, delay, numIter, partitions.length});

      // For each iteration...
      for (int iteration = 0; iteration < numIter; iteration++) {
        final Runtime r = Runtime.getRuntime();
        final long startMemory = (r.totalMemory() - r.freeMemory())/1048576;

        // Process the partial result and update to the cache


        final MasterCacheKey<DenseVector> parameterKey = vortexThreadPool.cache("param" + iteration, model);
        final long afterCache = (r.totalMemory() - r.freeMemory())/1048576;
        // Launch tasklets, each operating on a partition
        futures.clear();

        final CountDownLatch latch = new CountDownLatch(partitions.length);
        final AccuracyMeasurer measurer = new AccuracyMeasurer();
        for (int i = 0; i < partitions.length; i++) {
          final int finalIteration = iteration;
          vortexThreadPool.submit(new HDFSBackedGradientFunction(),
              new HDFSCachedInput(parameterKey, partitions[i]),
              new EventHandler<PartialResult>() {
                @Override
                public void onNext(final PartialResult result) {
                  processResult(model, result, finalIteration, measurer);
                  latch.countDown();
                }
              });
        }
        latch.await();
        LOG.log(Level.INFO, "@V@iteration\t{0}\taccuracy\t{1}\tHost\t{2}\tUsed\t{3}->{4}->{5}\tMax\t{6}\tTotal\t{7}",
            new Object[]{
                iteration, measurer.getAccuracy(),
                InetAddress.getLocalHost().getHostName(), startMemory, afterCache,
                (r.totalMemory() - r.freeMemory())/1048576, r.maxMemory()/1048576, r.totalMemory()/1048576});
      }
      final long duration = System.currentTimeMillis() - start;
      LOG.log(Level.INFO, "#V#finish\t{0}", duration);
    } catch (final Exception e) {
      final long duration = System.currentTimeMillis() - start;
      LOG.log(Level.WARNING, "#V#failed after " + duration, e);
    }
  }

  private synchronized void processResult(final DenseVector previousModel, final PartialResult result,
                                          final int iteration, final AccuracyMeasurer measurer) {
    final long startTime = System.currentTimeMillis();

    final double stepSize = 0.00001;
    final float thisIterStepSize = (float) (stepSize / Math.sqrt(numIter));

     measurer.add(result.getCount(), result.getNumPositive());

     // SimpleUpdater. No regularization
     previousModel.axpy(-thisIterStepSize, result.getCumGradient());

     final long endTime = System.currentTimeMillis();
     LOG.log(Level.INFO, "Processing time {0} in iteration{1}", new Object[] {endTime - startTime, iteration});
  }

  static class AccuracyMeasurer {
    int numTotal;
    int numPositive;

    AccuracyMeasurer() {
      this.numTotal = 0;
      this.numPositive = 0;
    }

    void add(final int numTotal, final int numPositive) {
      this.numTotal += numTotal;
      this.numPositive += numPositive;
    }

    double getAccuracy() {
      return (double) numPositive / numTotal;
    }
  }
}
