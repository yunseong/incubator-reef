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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.reef.vortex.api.VortexFuture;
import org.apache.reef.vortex.api.VortexStart;
import org.apache.reef.vortex.api.VortexThreadPool;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Logistic Regression User Code Example.
 */
final class LogisticRegressionStart implements VortexStart {
  private static final Logger LOG = Logger.getLogger(LogisticRegressionStart.class.getName());
  private static final int NUMBER_OF_TRAINING_DATA_INSTANCES = 1000 * 500 * 32;
  private static final int NUMBER_OF_ITERATIONS = 3;

  private static final int DIVIDE_FACTOR = 128;

  @Inject
  private LogisticRegressionStart() {
  }

  /**
   * Perform a simple vector multiplication on Vortex.
   */
  @Override
  public void start(final VortexThreadPool vortexThreadPool) {
    // [Theta0(corresponds to X0=1), Theta1(corresponds to X1), Theta2(corresponds to X2)]
    final Double[] initialParameters = {0.0, 0.0, 0.0};
    final Vector<Double> parameterVector = new Vector<>(Arrays.asList(initialParameters));

    // [X1(=tumorSize), X2(=age), Y(=isTumor?)]
    final Double[] noTumor = {-0.5, 0.5, 0.0};
    final Vector<Double> positiveVector = new Vector<>(Arrays.asList(noTumor));
    final Double[] tumor = {0.5, -0.5, 1.0};
    final Vector<Double> negativeVector = new Vector<>(Arrays.asList(tumor));

    // Create one TrainingData partition (ASSUMING THAT ALL PARTITIONS ARE THE SAME)
    final ArrayList<Vector<Double>> partition = new ArrayList<>();
    for (int i = 0; i < NUMBER_OF_TRAINING_DATA_INSTANCES/DIVIDE_FACTOR; i++) {
      if (i % 2 == 0) {
        partition.add(positiveVector);
      } else {
        partition.add(negativeVector);
      }
    }

    // Measure job finish time from here
    final double start = System.currentTimeMillis();

    // For each iteration...
    for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
      System.out.println(i + " Before Iteration: " + parameterVector);

      // Launch tasklets, each operating on a partition
      final ArrayList<VortexFuture<Vector<Double>>> futures = new ArrayList<>();
      for (int j = 0; j < DIVIDE_FACTOR; j++) {
        futures.add(vortexThreadPool.submit(new GradientFunction(),
            new ImmutablePair<>(parameterVector, partition)));
      }

      // Get the sum of partial gradients
      final Vector<Double> sumOfPartialGradients = new Vector<>();
      for (int l = 0; l < parameterVector.size(); l++) {
        sumOfPartialGradients.add(0.0);
      }
      for (final VortexFuture<Vector<Double>> future : futures) {
        try {
          final Vector<Double> gradient = future.get();
          assert(gradient.size() == sumOfPartialGradients.size());
          for (int k = 0; k < sumOfPartialGradients.size(); k++) {
            sumOfPartialGradients.setElementAt(sumOfPartialGradients.get(k) + gradient.get(k), k);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      // Update the parameters
      assert(parameterVector.size() == sumOfPartialGradients.size());
      for (int k = 0; k < parameterVector.size(); k++) {
        parameterVector.setElementAt(
            parameterVector.get(k) - (sumOfPartialGradients.get(k)/NUMBER_OF_TRAINING_DATA_INSTANCES/100), k
        );
      }
    }

    System.out.println("Final result: " + parameterVector);

    LOG.log(Level.INFO, "Job Finish Time: " + (System.currentTimeMillis() - start));
  }
}
