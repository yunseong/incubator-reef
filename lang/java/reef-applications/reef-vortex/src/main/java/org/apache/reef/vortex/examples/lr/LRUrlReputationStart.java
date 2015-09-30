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

  private final String dir;
  private final int numIter;
  private final int numFile;

  private final int divideFactor;

  private final int modelDim;
  private final boolean cached;
  private final double crashProb;
  private final int crashTimeout;

  @Inject
  private LRUrlReputationStart(@Parameter(LogisticRegression.DivideFactor.class) final int divideFactor,
                               @Parameter(LogisticRegression.NumIter.class) final int numIter,
                               @Parameter(LogisticRegression.NumFile.class) final int numFile,
                               @Parameter(LogisticRegression.Dir.class) final String dir,
                               @Parameter(LogisticRegression.ModelDim.class) final int modelDim,
                               @Parameter(LogisticRegression.CrashProb.class) final double crashProb,
                               @Parameter(LogisticRegression.CrashTimeout.class) final int crashTimeout,
                               @Parameter(LogisticRegression.Cached.class) final boolean cached) {
    this.divideFactor = divideFactor;
    this.numIter = numIter;
    this.numFile = numFile;
    this.dir = dir;
    this.modelDim = modelDim;
    this.crashProb = crashProb;
    this.crashTimeout = crashTimeout;
    this.cached = cached;
  }

  /**
   * Perform a simple vector multiplication on Vortex.
   */
  @Override
  public void start(final VortexThreadPool vortexThreadPool) {
    LOG.log(Level.INFO,
        "#V# DIVIDE_FACTOR {0} / NUM_ITER {1} / NUM_FILE {2} / CRASH_PROB {3} / CRASH_TIMEOUT {4} / CACHE {5}",
        new Object[]{divideFactor, numIter, numFile, crashProb, crashTimeout, cached});

    SparseVector parameterVector = new SparseVector(modelDim);

    // Measure job finish time from here
    final long start = System.currentTimeMillis();

    try {
      final ArrayList<TrainingData> partitions = parse();
      final long parseOverhead = System.currentTimeMillis() - start;

      final ArrayList<CacheKey<TrainingData>> partitionKeys =
          cachePartitions(vortexThreadPool, partitions);
      final long cacheOverhead = cached ? System.currentTimeMillis() - start - parseOverhead : 0;

      // For each iteration...
      for (int iter = 0; iter < numIter; iter++) {

        final CacheKey<SparseVector> parameterKey = vortexThreadPool.cache("param"+iter, parameterVector);
        PartialResult reducedResult = null;

        // Launch tasklets, each operating on a partition
        final ArrayList<VortexFuture<PartialResult>> futures = new ArrayList<>();
        for (int pIndex = 0; pIndex < divideFactor; pIndex++) {
          if (cached) {
            futures.add(vortexThreadPool.submit(
                new FullyCachedGradientFunction(),
                new LRInputCached(parameterKey, partitionKeys.get(pIndex))));
          } else {
            futures.add(vortexThreadPool.submit(
                new GradientFunction(),
                new LRInputNotCached(parameterVector, partitions.get(pIndex))));
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
          parameterVector = reducedResult.getPartialGradient().nTimes(0.0 - 1.0 / reducedResult.getCount());
//            final int numRecord = partitions.size();
//            final double learningRate = 1.0 / numRecord;
//            parameterVector.addVector(sumOfPartialGradients.nTimes(0.0 - learningRate));
//          LOG.log(Level.INFO, "#V# Iteration {0} / Accuracy: {1}", new Object[]{iter, accuracy});
        }
      }

      final long duration = System.currentTimeMillis() - start;
      final JobSummary summary = new JobSummary(duration, parseOverhead, cacheOverhead);
      LOG.log(Level.INFO, "#V# Job finished. {0}", summary);
    } catch (final Exception e) {
      final long duration = System.currentTimeMillis() - start;
      final JobSummary summary = new JobSummary(duration, -1, -1);
      LOG.log(Level.SEVERE, "#V# Job failed. " + summary, e);
    }
  }

  /**
   * Cache the partitions into Vortex Cache.
   * @return The cached keys
   */
  private ArrayList<CacheKey<TrainingData>> cachePartitions(final VortexThreadPool vortexThreadPool,
                                                            final ArrayList<TrainingData> partitions)
      throws VortexCacheException {

    final ArrayList<CacheKey<TrainingData>> keys = new ArrayList<>(divideFactor);
    for (int i = 0; i < partitions.size(); i++) {
      keys.add(vortexThreadPool.cache(i + "", partitions.get(i)));
    }
    return keys;
  }

  /**
   * Read lines from the N files, and split into the partitions as many as specified in the divideFactor.
   * @return the partitions that consist of the records.
   * @throws IOException If it fails while parsing the input.
   */
  private ArrayList<TrainingData> parse() throws IOException {
    final ArrayList<TrainingData> partitions = new ArrayList<>(divideFactor);
    for (int i = 0; i < divideFactor; i++) {
      partitions.add(new TrainingData());
    }


    long recordCount = 0;
    for (int fileIndex = 0; fileIndex < numFile; fileIndex++) {
      final String path = dir + "Day" + fileIndex + ".svm"; // e.g., dir/Day13.svm
      LOG.log(Level.INFO, "Path: {0}", path);

      try (final BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)))) {
        String line;
        while ((line = reader.readLine()) != null){
          final int index = (int) (recordCount % divideFactor);
          partitions.get(index).addRow(parseLine(line));
          recordCount++;
        }
      }
    }
    return partitions;
  }

  /**
   * Parse a line and create a training data.
   */
  private Row parseLine(final String line) throws ParseException {
    final SparseVector feature = new SparseVector(modelDim);

    final String[] split = line.split(" ");

    try {
      final int output = Integer.valueOf(split[0]);

      for (int i = 1; i < split.length; i++) {
        final String[] column = split[i].split(":");

        final int index = Integer.valueOf(column[0]);
        final double value = Double.valueOf(column[1]);

        if (index >= modelDim) {
          // Restrict the dimension of model to save our time.
          break;
        }
        feature.putValue(index, value);
      }
      return Row.getInstance(feature, output);

    } catch (final NumberFormatException e) {
      throw new ParseException(e.getMessage());
    }
  }
}
