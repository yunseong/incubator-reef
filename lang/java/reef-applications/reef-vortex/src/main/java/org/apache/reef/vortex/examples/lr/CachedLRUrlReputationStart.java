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
import org.apache.reef.vortex.common.CacheKey;
import org.apache.reef.vortex.common.exceptions.VortexCacheException;
import org.apache.reef.vortex.examples.lr.input.ArrayBasedVector;
import org.apache.reef.vortex.examples.lr.input.LRInputCached;
import org.apache.reef.vortex.examples.lr.input.ParseException;
import org.apache.reef.vortex.examples.lr.input.SparseVector;
import org.apache.reef.vortex.failure.parameters.IntervalMs;
import org.apache.reef.vortex.failure.parameters.Probability;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Logistic Regression User Code Example using URL Reputation Data Set.
 * http://archive.ics.uci.edu/ml/machine-learning-databases/url/url.names
 * The model and training data is cached
 */
final class CachedLRUrlReputationStart implements VortexStart {
  private static final Logger LOG = Logger.getLogger(CachedLRUrlReputationStart.class.getName());

  private final String path;
  private final int numIter;

  private final int divideFactor;
  private final int modelDim;
  private final int numRecords;

  // For printing purpose actually.
  private final double probability;
  private final int interval;

  private final List<CacheKey<ArrayList<ArrayBasedVector>>> partitions;

  @Inject
  private CachedLRUrlReputationStart(@Parameter(LogisticRegression.DivideFactor.class) final int divideFactor,
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
    this.partitions = new ArrayList<>(divideFactor);
  }

  /**
   * Perform a simple vector multiplication on Vortex.
   */
  @Override
  public void start(final VortexThreadPool vortexThreadPool) {
    LOG.log(Level.INFO,
        "#V#startCached\tDIVIDE_FACTOR\t{0}\tCRASH_PROB\t{1}\tCRASH_INTERVAL\t{2}\tNUM_ITER\t{3}\tNUM_RECORDS\t{4}",
        new Object[]{divideFactor, probability, interval, numIter, numRecords});

    // Measure job finish time from here
    final long start = System.currentTimeMillis();

    try {
      int iteration = 0;
      SparseVector model = new SparseVector(modelDim);
      final CacheKey<SparseVector> initialModelKey = vortexThreadPool.cache("param" + iteration, model);
      iteration++;

      final List<VortexFuture<PartialResult>> initialResult = submitInitialTasklets(vortexThreadPool, initialModelKey);
      final long parseOverhead = System.currentTimeMillis() - start;
      LOG.log(Level.INFO, "Parsing overhead {0} ms for {1} records", new Object[]{parseOverhead, numRecords});
      List<VortexFuture<PartialResult>> futures = initialResult;

      // For each iteration...
      for (; iteration < numIter; iteration++) {
        // Process the partial result and update to the cache
        model = processResult(futures, iteration);

        final CacheKey<SparseVector> parameterKey = vortexThreadPool.cache("param" + iteration, model);
        // Launch tasklets, each operating on a partition
        futures.clear();
        for (final CacheKey<ArrayList<ArrayBasedVector>> partition : partitions) {
          futures.add(vortexThreadPool.submit(
              new CachedGradientFunction(),
              new LRInputCached(parameterKey, partition, modelDim)));
        }
      }

      if (iteration == numIter) {
        processResult(futures, iteration);
      }

      final long duration = System.currentTimeMillis() - start;
      final JobSummary summary = new JobSummary(duration, parseOverhead);
      LOG.log(Level.INFO, "#V#finish\t{0}", summary);
    } catch (final Exception e) {
      final long duration = System.currentTimeMillis() - start;
      LOG.log(Level.WARNING, "#V#failed after " + duration, e);
    }
  }

  /**
   * Aggregate the partial results, compute accuracy, and update model.
   * @return Updated model.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private SparseVector processResult(final Collection<VortexFuture<PartialResult>> futures, final int iteration)
      throws ExecutionException, InterruptedException {
    PartialResult reducedResult = null;
    for (final VortexFuture<PartialResult> future : futures) {
      final PartialResult partialResult = future.get();
      if (reducedResult == null) {
        reducedResult = partialResult;
      } else {
        reducedResult.addResult(partialResult);
      }
    }

    if (reducedResult == null) {
      LOG.log(Level.WARNING, "The partial result has not been not reduced correctly in iteration {0}", iteration);
      throw new RuntimeException("Iteration " + iteration + " has failed");
    } else {
      final double accuracy = ((double) reducedResult.getNumPositive()) / reducedResult.getCount();
      LOG.log(Level.INFO, "@V@iteration\t{0}\taccuracy\t{1}", new Object[]{iteration, accuracy});
      return reducedResult.getPartialGradient().nTimes(1.0f / reducedResult.getCount());
    }
  }

  /**
   * Submit the tasklets at the initial iteration. Tasklets are submitted right after parsing each partition.
   * @return Futures that will return results.
   */
  private List<VortexFuture<PartialResult>> submitInitialTasklets(final VortexThreadPool threadPool,
                                                                  final CacheKey modelKey)
      throws IOException, VortexCacheException {

    final List<VortexFuture<PartialResult>> futures = new ArrayList<>(divideFactor);

    try (final BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)))) {
      final int partitionSize = (numRecords + divideFactor - 1) / divideFactor;
      final ArrayList<ArrayBasedVector> vectors = new ArrayList<>(partitionSize);

      String line;
      while ((line = reader.readLine()) != null) {
        final ArrayBasedVector vector = parseLine(line);
        vectors.add(vector);

        if (vectors.size() == partitionSize) {
          futures.add(cacheAndSubmit(modelKey, vectors, threadPool));
          vectors.clear();
        }
      }

      // Submit remaining tasklets.
      if (!vectors.isEmpty()) {
        futures.add(cacheAndSubmit(modelKey, vectors, threadPool));
      }
    }
    return futures;
  }

  /**
   * Cache the training data and submit the VortexFunction.
   * @return Result of gradient function.
   * @throws VortexCacheException
   */
  private VortexFuture<PartialResult> cacheAndSubmit(final CacheKey modelKey,
                                                     final ArrayList<ArrayBasedVector> vectors,
                                                     final VortexThreadPool threadPool) throws VortexCacheException {
    // Cache the partition
    final CacheKey<ArrayList<ArrayBasedVector>> key =
        threadPool.cache("partition" + Integer.toString(partitions.size()), vectors);
    partitions.add(key);

    // Submit the tasklet
    final LRInputCached input = new LRInputCached(modelKey, key, modelDim);
    return threadPool.submit(new CachedGradientFunction(), input);
  }

  /**
   * Parse a line and create a training data.
   */
  private static ArrayBasedVector parseLine(final String line) throws ParseException {
    final String[] split = line.split(" ");

    try {
      final int output = Integer.valueOf(split[0]);
      final int[] indices = new int[split.length - 1];
      final float[] values = new float[split.length - 1];

      for (int i = 1; i < split.length; i++) {
        final String[] column = split[i].split(":");

        final int index = Integer.valueOf(column[0]);
        final float value = Float.valueOf(column[1]);

        indices[i-1] = index;
        values[i-1] = value;
      }
      return new ArrayBasedVector(values, indices, output);

    } catch (final NumberFormatException e) {
      throw new ParseException(e.getMessage());
    }
  }
}
