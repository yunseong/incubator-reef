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
import org.apache.reef.vortex.examples.lr.input.*;
import org.apache.reef.vortex.failure.parameters.Interval;
import org.apache.reef.vortex.failure.parameters.Probability;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Logistic Regression User Code Example using URL Reputation Data Set.
 * http://archive.ics.uci.edu/ml/machine-learning-databases/url/url.names
 */
final class LRUrlReputationStart implements VortexStart {
  private static final Logger LOG = Logger.getLogger(LRUrlReputationStart.class.getName());
  private static final String CACHE_FULL = "full";
  private static final String CACHE_HALF = "half";
  private static final String CACHE_NO = "no";

  private final String dir;
  private final int numIter;
  private final int numFile;

  private final int divideFactor;

  private final int modelDim;
  private final String cached;
  private final double probability;
  private final int interval;

  @Inject
  private LRUrlReputationStart(@Parameter(LogisticRegression.DivideFactor.class) final int divideFactor,
                               @Parameter(LogisticRegression.NumIter.class) final int numIter,
                               @Parameter(LogisticRegression.NumFile.class) final int numFile,
                               @Parameter(LogisticRegression.Dir.class) final String dir,
                               @Parameter(LogisticRegression.ModelDim.class) final int modelDim,
                               @Parameter(LogisticRegression.Cache.class) final String cached,
                               @Parameter(Probability.class) final double probability,
                               @Parameter(Interval.class) final int interval) {
    this.divideFactor = divideFactor;
    this.numIter = numIter;
    this.numFile = numFile;
    this.dir = dir;
    this.modelDim = modelDim;
    this.cached = cached;
    this.probability = probability;
    this.interval = interval;
  }

  /**
   * Perform a simple vector multiplication on Vortex.
   */
  @Override
  public void start(final VortexThreadPool vortexThreadPool) {
    LOG.log(Level.INFO,
        "#V#start\tDIVIDE_FACTOR\t{0}\tCRASH_PROB\t{1}\tCACHE\t{2}\tCRASH_INTERVAL\t{3}\tNUM_ITER\t{4}\tNUM_FILE\t{5}",
        new Object[]{divideFactor, probability, cached, interval, numIter, numFile});

    SparseVector parameterVector = new SparseVector(modelDim);

    // Measure job finish time from here
    final long start = System.currentTimeMillis();

    try {
      final ArrayList<ArrayList<ArrayBasedVector>> partitions = parse();
      final long parseOverhead = System.currentTimeMillis() - start;

      final ArrayList<CacheKey<ArrayList<ArrayBasedVector>>> partitionKeys =
          cachePartitions(vortexThreadPool, partitions);

      // For each iteration...
      for (int iter = 0; iter < numIter; iter++) {

        final CacheKey<SparseVector> parameterKey = vortexThreadPool.cache("param"+iter, parameterVector);
        PartialResult reducedResult = null;

        // Launch tasklets, each operating on a partition
        final ArrayList<VortexFuture<PartialResult>> futures = new ArrayList<>();
        for (int pIndex = 0; pIndex < divideFactor; pIndex++) {
          if (CACHE_FULL.equals(cached)) {
            futures.add(vortexThreadPool.submit(
                new CachedGradientFunction(),
                new LRInputCached(parameterKey, partitionKeys.get(pIndex), modelDim)));
          } else if (CACHE_HALF.equals(cached)) {
            futures.add(vortexThreadPool.submit(
                new HalfCachedGradientFunction(),
                new LRInputHalfCached(parameterVector, partitionKeys.get(pIndex), modelDim)));
          } else if (CACHE_NO.equals(cached)) {
            futures.add(vortexThreadPool.submit(
                new GradientFunction(),
                new LRInput(parameterVector, partitions.get(pIndex), modelDim)));
          } else {
            throw new RuntimeException("Unknown type");
          }
        }

        for (final VortexFuture<PartialResult> future : futures) {
          final PartialResult partialResult = future.get();
          if (reducedResult == null) {
            reducedResult = partialResult;
          } else {
            reducedResult.addResult(partialResult);
          }
        }

        if (reducedResult == null) {
          LOG.log(Level.WARNING, "The partial result has not been not reduced correctly in iteration {0}", iter);
        } else {
          final double accuracy = ((double) reducedResult.getNumPositive()) / reducedResult.getCount();
          parameterVector = reducedResult.getPartialGradient().nTimes(1.0f / reducedResult.getCount());
          LOG.log(Level.INFO, "@V@iter\t{0}\taccuracy\t{1}", new Object[]{iter, accuracy});
        }
      }

      final long duration = System.currentTimeMillis() - start;
      final JobSummary summary = new JobSummary(duration, parseOverhead);
      LOG.log(Level.INFO, "#V#finish\t{0}", summary);
    } catch (final Exception e) {
      final long duration = System.currentTimeMillis() - start;
      final JobSummary summary = new JobSummary(duration, -1);
      LOG.log(Level.SEVERE, "#V#failed\t" + summary, e);
    }
  }

  /**
   * Cache the partitions into Vortex Cache.
   * @return The cached keys
   */
  private ArrayList<CacheKey<ArrayList<ArrayBasedVector>>>
      cachePartitions(final VortexThreadPool vortexThreadPool,
                      final ArrayList<ArrayList<ArrayBasedVector>> partitions) throws VortexCacheException {

    final ArrayList<CacheKey<ArrayList<ArrayBasedVector>>> keys = new ArrayList<>(divideFactor);
    for (int i = 0; i < partitions.size(); i++) {
      keys.add(vortexThreadPool.cache(String.valueOf(i), partitions.get(i)));
    }
    return keys;
  }

  /**
   * Read lines from the N files, and split into the partitions as many as specified in the divideFactor.
   * @return the partitions that consist of the records.
   * @throws IOException If it fails while parsing the input.
   */
  private ArrayList<ArrayList<ArrayBasedVector>> parse() throws IOException {
    final ArrayList<ArrayList<ArrayBasedVector>> partitions = new ArrayList<>(divideFactor);
    for (int i = 0; i < divideFactor; i++) {
      partitions.add(new ArrayList<ArrayBasedVector>());
    }

    long recordCount = 0;
    for (int fileIndex = 0; fileIndex < numFile; fileIndex++) {
      final String path = dir + "Day" + fileIndex + ".svm"; // e.g., dir/Day13.svm
      LOG.log(Level.INFO, "Path: {0}", path);

      try (final BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)))) {
        String line;
        while ((line = reader.readLine()) != null) {
          final int index = (int) (recordCount % divideFactor);
          final ArrayBasedVector vector = parseLine(line, modelDim);
          partitions.get(index).add(vector);
          recordCount++;
        }
      }
    }
    return partitions;
  }

  /**
   * Parse a line and create a training data.
   */
  private static ArrayBasedVector parseLine(final String line, final int modelDim) throws ParseException {
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
