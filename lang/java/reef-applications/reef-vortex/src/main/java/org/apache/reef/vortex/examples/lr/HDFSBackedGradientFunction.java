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

import java.net.InetAddress;
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
    final Runtime r = Runtime.getRuntime();
    final long startTime = System.currentTimeMillis();
    final long startMemory = (r.totalMemory() - r.freeMemory())/1048576;

    final SparseVector parameterVector = lrInput.getParameterVector();
    final long modelLoadedTime = System.currentTimeMillis();
    final long modelLoadedMemory = (r.totalMemory() - r.freeMemory())/1048576;

    final List<ArrayBasedVector> trainingData = lrInput.getTrainingData();
    final long trainingLoadedTime = System.currentTimeMillis();
    final long trainingLoadedMemory = (r.totalMemory() - r.freeMemory())/1048576;

    LOG.log(Level.INFO, "!V!Init\t{0}\tUsed\t{1}->{2}->{3}\tMax\t{4}\tTotal\t{5}",
        new Object[] {InetAddress.getLocalHost().getHostName(),
            startMemory, modelLoadedMemory, trainingLoadedMemory, r.maxMemory(), r.totalMemory()});

    final SparseVector cumGradient = new SparseVector(parameterVector.getDimension());
    final PartialResult partialResult = new PartialResult(cumGradient);

    for (final ArrayBasedVector instance : trainingData) {

      final double predict = parameterVector.dot(instance);
      final double label = instance.getOutput();
      final boolean isPositive = predict * label > 0;

      // Update the gradient vector.
      /*
      final double margin = - 1.0 * predict;
      final double multiplier = (1.0 / (1.0 + Math.exp(margin))) - label;
      cumGradient.axpy(multiplier, instance);

      final double loss = label > 0 ? log1pExp(margin) : log1pExp(margin) - margin;

      */
      final double exponent = - predict * label;
      final double maxExponent = Math.max(exponent, 0);
      final double logSumExp = maxExponent + Math.log(Math.exp(-maxExponent) + Math.exp(exponent - maxExponent));
      final double multiplier = label * (Math.exp(-logSumExp) - 1);
      cumGradient.axpy(multiplier, instance);

      partialResult.addResult(cumGradient, 0.0, isPositive);
    }

    final long finishTime = System.currentTimeMillis();
    final long executionTime = finishTime - trainingLoadedTime;
    final long modelOverhead = modelLoadedTime - startTime;
    final long trainingOverhead = trainingLoadedTime - modelLoadedTime;

    LOG.log(Level.INFO, "!V!\t{0}\tUsed\t{1}->{2}\tMax\t{3}\tTotal\t{4}" +
        "\tExecution\t{5}\tModel\t{6}\tTraining\t{7}\tTrainingNum\t{8}\tkey\t{9}",
        new Object[]{
            InetAddress.getLocalHost().getHostName(), startMemory, (r.totalMemory() - r.freeMemory())/1048576,
            r.maxMemory()/1048576, r.totalMemory()/1048576,
            executionTime, modelOverhead, trainingOverhead, trainingData.size(),
            Arrays.toString(lrInput.getCachedKeys().toArray())});

    return partialResult;
  }

  /**
   * To avoid overflow computing math.log(1 + math.exp(x)).
   * @param x
   * @return
   */
  private double log1pExp(final double x) {
    if (x > 0) {
      return x + Math.log1p(Math.exp(-x));
    } else {
      return Math.log1p(Math.exp(x));
    }
  }
}
