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

import org.apache.reef.vortex.api.VortexFuture;
import org.apache.reef.vortex.api.VortexStart;
import org.apache.reef.vortex.api.VortexThreadPool;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
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
  private static final int NUMBER_OF_TRAINING_DATA_INSTANCES = 1000 * 32 * 20;
  private static final int NUMBER_OF_ITERATIONS = 10;

  private static final int DIVIDE_FACTOR = 8;

  @Inject
  private LogisticRegressionStart() {
  }

  /**
   * Perform a simple vector multiplication on Vortex.
   */
  @Override
  public void start(final VortexThreadPool vortexThreadPool) {
    String path;
    if (System.getProperty("os.name").toLowerCase().contains("linux")) {
      path = "/home/azureuser/data/lr-input.txt";
    } else if (System.getProperty("os.name").toLowerCase().contains("windows")){
      path = "C:\\Users\\v-yunlee\\lr-input.txt";
    } else {
      path = "";
    }

    // [Theta0(corresponds to X0=1), Theta1(corresponds to X1), Theta2(corresponds to X2)]
    final Double[] initialParameters = {0.0, 0.0, 0.0};
    final Vector<Double> parameterVector = new Vector<>(Arrays.asList(initialParameters));

    // Measure job finish time from here
    final double start = System.currentTimeMillis();

//    final ArrayList<CacheKey> trainingDataKeys = new ArrayList<>(DIVIDE_FACTOR);
    final ArrayList<ArrayList<Vector<Double>>> partitions = new ArrayList<>(DIVIDE_FACTOR);

    try {

      final FileInputStream inputStream = new FileInputStream(path);
      final BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

      // Caches the initial training data
      for (int j = 0; j < DIVIDE_FACTOR; j++) {

        // Get the next partition
        final ArrayList<Vector<Double>> partition = new ArrayList<>();
        for (int k = 0; k < NUMBER_OF_TRAINING_DATA_INSTANCES / DIVIDE_FACTOR; k++) {
          final String strLine = br.readLine();
          final String[] numbers = strLine.split(" ");
          final double x1 = Double.valueOf(numbers[0]);
          final double x2 = Double.valueOf(numbers[1]);
          final double y = Double.valueOf(numbers[2]);

          final Vector<Double> vector = new Vector<>();
          vector.add(x1);
          vector.add(x2);
          vector.add(y);
          partition.add(vector);
        }

//        trainingDataKeys.add(vortexThreadPool.cache("trainingData" + j, partition));
        partitions.add(partition);
      }

      // For each iteration...
      for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
        System.out.println(i + " Before Iteration: " + parameterVector);

//        final CacheKey<Vector<Double>> parameterVectorKey = vortexThreadPool.cache("param" + i, parameterVector);

        // Launch tasklets, each operating on a partition
        final ArrayList<VortexFuture<Vector<Double>>> futures = new ArrayList<>();
        for (int j = 0; j < DIVIDE_FACTOR; j++) {
          futures.add(vortexThreadPool.submit(
              new GradientFunction(),
              new LRInputNotCached(parameterVector, partitions.get(j))));
//              new LRInputHalfCached(parameterVector, trainingDataKeys.get(j))));
//                new LogisticRegressionInput(parameterVectorKey, trainingDataKeys.get(j))));
        }

        // Get the sum of partial gradients
        final Vector<Double> sumOfPartialGradients = new Vector<>();
        for (int l = 0; l < parameterVector.size(); l++) {
          sumOfPartialGradients.add(0.0);
        }
        for (final VortexFuture<Vector<Double>> future : futures) {
          final Vector<Double> gradient = future.get();
          assert (gradient.size() == sumOfPartialGradients.size());
          for (int k = 0; k < sumOfPartialGradients.size(); k++) {
            sumOfPartialGradients.setElementAt(sumOfPartialGradients.get(k) + gradient.get(k), k);
          }
        }

        // Update the parameters
        assert (parameterVector.size() == sumOfPartialGradients.size());
        for (int k = 0; k < parameterVector.size(); k++) {
          parameterVector.setElementAt(
              parameterVector.get(k) - (sumOfPartialGradients.get(k) / NUMBER_OF_TRAINING_DATA_INSTANCES / 100), k
          );
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    System.out.println("Final result: " + parameterVector);

    LOG.log(Level.INFO, "Job Finish Time: " + (System.currentTimeMillis() - start));
  }
}
