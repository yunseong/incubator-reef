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

import org.apache.reef.vortex.api.VortexFunction;
import org.apache.reef.vortex.examples.lr.input.ArrayBasedVector;
import org.apache.reef.vortex.examples.lr.input.HDFSCachedInput;
import org.apache.reef.vortex.examples.lr.input.SparseVector;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Outputs the gradient.
 */
final class HDFSBackedGradientFunction implements VortexFunction<HDFSCachedInput, PartialResult> {
  private static final Logger LOG = Logger.getLogger(HDFSBackedGradientFunction.class.getName());
  /**
   * Outputs the gradient.
   */
  @Override
  public PartialResult call(final HDFSCachedInput lrInput) throws Exception {
    final long startTime = System.currentTimeMillis();

    final SparseVector parameterVector = lrInput.getParameterVector();
    final long modelLoadedTime = System.currentTimeMillis();

    final List<ArrayBasedVector> trainingData = lrInput.getTrainingData();
    final long trainingLoadedTime = System.currentTimeMillis();

    final SparseVector gradientResult = new SparseVector(parameterVector.getDimension());

    // For estimating the accuracy.
    int numPositive = 0;
    int numNegative = 0;

    for (final ArrayBasedVector instance : trainingData) {
      final double predict = parameterVector.dot(instance);
      final double y = instance.getOutput();
      if (predict * y > 0) {
        numPositive++;
      } else {
        numNegative++;
      }
      // Update the gradient vector.
      final double exponent = -predict * y;
      final double maxExponent = Math.max(exponent, 0);
      final double lambda = 1.0; // regularization
      final double logSumExp = maxExponent + Math.log(Math.exp(-maxExponent) + Math.exp(exponent - maxExponent));
      final double multiplier = y * (Math.exp(-logSumExp) - 1);
      final float stepSize = 0.00001f;
      gradientResult.addVector((float) (-stepSize * multiplier), instance);
    }

    final long finishTime = System.currentTimeMillis();
    final long executionTime = finishTime - trainingLoadedTime;
    final long modelOverhead = modelLoadedTime - startTime;
    final long trainingOverhead = trainingLoadedTime - modelLoadedTime;
    LOG.log(Level.INFO, "!V!\tExecution\t{0}\tModel\t{1}\tTraining\t{2}\tTrainingNum\t{3}\tkey\t{4}",
        new Object[]{executionTime, modelOverhead, trainingOverhead, trainingData.size(),
            Arrays.toString(lrInput.getCachedKeys().toArray())});
    return new PartialResult(gradientResult, numPositive, numNegative);
  }

  /**
   * @param predict inner product of the gradient and instance.
   * @return
   */
  private double getHypothesis(final double predict) {
    final double exponent = -1.0 * predict;
    return 1 / (1 + Math.pow(Math.E, exponent));
  }
}
