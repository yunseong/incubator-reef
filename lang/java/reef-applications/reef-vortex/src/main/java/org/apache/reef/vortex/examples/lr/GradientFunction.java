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

import org.apache.reef.io.serialization.Codec;
import org.apache.reef.vortex.api.VortexFunction;
import org.apache.reef.vortex.examples.lr.input.GradientFunctionInput;
import org.apache.reef.vortex.examples.lr.input.InputCodec;
import org.apache.reef.vortex.examples.lr.output.GradientFunctionOutput;
import org.apache.reef.vortex.examples.lr.output.OutputCodec;
import org.apache.reef.vortex.examples.lr.vector.DenseVector;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Outputs the gradient.
 */
public final class GradientFunction implements VortexFunction<GradientFunctionInput, GradientFunctionOutput> {
  private static final Logger LOG = Logger.getLogger(GradientFunction.class.getName());
  /**
   * Outputs the gradient.
   */
  @Override
  public GradientFunctionOutput call(final GradientFunctionInput lrInput) throws Exception {
    final long startTime = System.currentTimeMillis();

    final DenseVector parameterVector = lrInput.getParameterVector();
    final long modelLoadedTime = System.currentTimeMillis();

    final List<Row> trainingData = lrInput.getTrainingData();
    final long trainingLoadedTime = System.currentTimeMillis();

    final DenseVector cumGradient = new DenseVector(parameterVector.getDimension());

    int numPositive = 0;
    for (final Row instance : trainingData) {

      final double predict = parameterVector.dot(instance.getFeature());
      final int label = instance.getOutput();
      if (predict * label > 0) {
        numPositive++;
      }

      // Update the gradient vector.
      /*
      final double margin = - 1.0 * predict;
      final double multiplier = (1.0 / (1.0 + Math.exp(margin))) - label;
      cumGradient.axpy(multiplier, instance);
      final double loss = label > 0 ? log1pExp(margin) : log1pExp(margin) - margin;
      */
      final double exponent = -predict * label;
      final double maxExponent = Math.max(exponent, 0);
      final double logSumExp = maxExponent + Math.log(Math.exp(-maxExponent) + Math.exp(exponent - maxExponent));
      final float multiplier = (float) (label * (Math.exp(-logSumExp) - 1));
      cumGradient.axpy(multiplier, instance.getFeature());
    }

    final long finishTime = System.currentTimeMillis();
    final long computeTime = finishTime - trainingLoadedTime;
    final long modelOverhead = modelLoadedTime - startTime;
    final long trainingOverhead = trainingLoadedTime - modelLoadedTime;

    LOG.log(Level.INFO, "!V!\t{0}\tTotal\t{1}\tCompute\t{2}\tModel\t{3}\tTraining\t{4}\tNumRecords\t{5}\tkey\t{6}",
        new Object[]{InetAddress.getLocalHost().getHostName(), finishTime - startTime, computeTime, modelOverhead,
            trainingOverhead, trainingData.size(), Arrays.toString(lrInput.getCachedKeys().toArray())});

    return new GradientFunctionOutput(cumGradient, numPositive, trainingData.size());
  }

  @Override
  public Codec<GradientFunctionInput> getInputCodec() {
    return new InputCodec();
  }

  @Override
  public Codec<GradientFunctionOutput> getOutputCodec() {
    return new OutputCodec();
  }
}