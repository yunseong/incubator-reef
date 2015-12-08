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

import edu.snu.utils.DVector;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.vortex.api.VortexFuture;
import org.apache.reef.vortex.api.VortexStart;
import org.apache.reef.vortex.api.VortexThreadPool;
import org.apache.reef.vortex.common.MasterCacheKey;
import org.apache.reef.vortex.common.HDFSBackedCacheKey;
import org.apache.reef.vortex.examples.lr.input.HDFSCachedInput;
import org.apache.reef.vortex.examples.lr.input.RowParser;
import org.apache.reef.vortex.examples.lr.input.DenseVector;
import org.apache.reef.vortex.examples.lr.input.VectorUtil;
import org.apache.reef.vortex.failure.parameters.IntervalMs;
import org.apache.reef.vortex.failure.parameters.Probability;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
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
  private final double probability;
  private final int interval;

  @Inject
  private HDFSLRUrlReputationStart(@Parameter(LogisticRegression.DivideFactor.class) final int divideFactor,
                                   @Parameter(LogisticRegression.NumIter.class) final int numIter,
                                   @Parameter(LogisticRegression.Path.class) final String path,
                                   @Parameter(LogisticRegression.ModelDim.class) final int modelDim,
                                   @Parameter(LogisticRegression.NumRecords.class) final int numRecords,
                                   @Parameter(Probability.class) final double probability,
                                   @Parameter(IntervalMs.class) final int interval) {
    this.divideFactor = divideFactor;
    this.numIter = numIter;
    this.path = path;
    this.modelDim = modelDim;
    this.numRecords = numRecords;
    this.probability = probability;
    this.interval = interval;
  }

  /**
   * Perform a simple vector multiplication on Vortex.
   */
  @Override
  public void start(final VortexThreadPool vortexThreadPool) {

    // Measure job finish time from here
    final long start = System.currentTimeMillis();

    try {
      DVector model = VectorUtil.toBreezeDense(new DenseVector(new double[modelDim]));

      final HDFSBackedCacheKey[] partitions = vortexThreadPool.cache(path, divideFactor,
          new RowParser());
      final List<VortexFuture<PartialResult>> futures = new ArrayList<>(partitions.length);

      LOG.log(Level.INFO,
          "#V#startCached\tDIVIDE_FACTOR\t{0}\tCRASH_PROB\t{1}\tCRASH_INTERVAL\t{2}\tNUM_ITER\t{3}\tSPLITS\t{4}",
          new Object[]{divideFactor, probability, interval, numIter, partitions.length});

      // For each iteration...
      for (int iteration = 0; iteration < numIter; iteration++) {
        // Process the partial result and update to the cache
        final MasterCacheKey<DenseVector> parameterKey = vortexThreadPool.cache("param" + iteration,
            VectorUtil.fromBreeze(model));
        // Launch tasklets, each operating on a partition
        futures.clear();

        for (int i = 0; i < partitions.length; i++) {
          futures.add(vortexThreadPool.submit(
              new HDFSBackedGradientFunction(),
              new HDFSCachedInput(parameterKey, partitions[i])));
        }
        processResult(model, futures, iteration);
      }

      final long duration = System.currentTimeMillis() - start;
      LOG.log(Level.INFO, "#V#finish\t{0}", duration);
    } catch (final Exception e) {
      final long duration = System.currentTimeMillis() - start;
      LOG.log(Level.WARNING, "#V#failed after " + duration, e);
    }
  }

  /**
   * Aggregate the partial results, compute accuracy, and update model.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void processResult(final DVector previousModel, final Collection<VortexFuture<PartialResult>> futures,
                             final int iteration)
      throws ExecutionException, InterruptedException {

    final double stepSize = 0.00001;
    final double thisIterStepSize = stepSize / Math.sqrt(numIter);

    int numTotal = 0;
    int numPositive = 0;
    for (final VortexFuture<PartialResult> future : futures) {
      final PartialResult partialResult = future.get();
      numTotal += partialResult.getCount();
      numPositive += partialResult.getNumPositive();

      // SimpleUpdater. No regularization
      previousModel.add(-thisIterStepSize, VectorUtil.toBreezeDense(partialResult.getCumGradient()));
    }

    final double accuracy = (double) numPositive / numTotal;
    LOG.log(Level.INFO, "@V@iteration\t{0}\taccuracy\t{1}", new Object[]{iteration, accuracy});
  }
}
