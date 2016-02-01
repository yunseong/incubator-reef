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
package org.apache.reef.vortex.examples.lr.multi;

import org.apache.reef.io.serialization.Codec;
import org.apache.reef.vortex.api.VortexFunction;
import org.apache.reef.vortex.examples.lr.Row;
import org.apache.reef.vortex.examples.lr.multi.input.MultiClassGradientFunctionInput;
import org.apache.reef.vortex.examples.lr.multi.input.MultiClassInputCodec;
import org.apache.reef.vortex.examples.lr.multi.output.MultiClassGradientFunctionOutput;
import org.apache.reef.vortex.examples.lr.vector.DenseVector;
import org.apache.reef.vortex.examples.lr.multi.output.MultiClassOutputCodec;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Outputs the gradient.
 */
public final class MultiClassGradientFunction
    implements VortexFunction<MultiClassGradientFunctionInput, MultiClassGradientFunctionOutput> {
  private static final Logger LOG = Logger.getLogger(MultiClassGradientFunction.class.getName());
  /**
   * Outputs the gradient.
   */
  @Override
  public MultiClassGradientFunctionOutput call(final MultiClassGradientFunctionInput lrInput) throws Exception {
    final long startTime = System.currentTimeMillis();

    final DenseVector[] parameterVector = lrInput.getParameterVector();
    final long modelLoadedTime = System.currentTimeMillis();

    final List<Row> trainingData = lrInput.getTrainingData();
    final long trainingLoadedTime = System.currentTimeMillis();

    final int numLabels = parameterVector.length;

    final DenseVector[] cumGradient = new DenseVector[numLabels];
    for (int i = 0; i < numLabels; i++) {
      cumGradient[i] = new DenseVector(parameterVector[i].getDimension());
    }

    int numPositive = 0;
    for (final Row instance : trainingData) {
      double[] predicts = new double[numLabels];

      // Iterate over all the labels.
      for (int i = 0; i < numLabels; i++) {
        final double predict = parameterVector[i].dot(instance.getFeature());
        predicts[i] = predict;

        final double label = (i + 1 == instance.getOutput()) ? 1.0 : -1.0; // Label starts from 1


        final double exponent = -predict * label;
        final double maxExponent = Math.max(exponent, 0);
        final double logSumExp = maxExponent + Math.log(Math.exp(-maxExponent) + Math.exp(exponent - maxExponent));
        final float multiplier = (float) (label * (Math.exp(-logSumExp) - 1));
        cumGradient[i].axpy(multiplier, instance.getFeature());
      }

      int maxIndex = 0;
      double maxPredict = -Double.MAX_VALUE;
      for (int i = 0; i < predicts.length; i++) {
        if (maxPredict < predicts[i]) {
          maxPredict = predicts[i];
          maxIndex = i + 1;
        }
      }

      // If the class is same!
      if (maxIndex == instance.getOutput()) {
        numPositive++;
      }
    }

    final long finishTime = System.currentTimeMillis();
    final long computeTime = finishTime - trainingLoadedTime;
    final long modelOverhead = modelLoadedTime - startTime;
    final long trainingOverhead = trainingLoadedTime - modelLoadedTime;

    LOG.log(Level.INFO, "!V!\t{0}\tTotal\t{1}\tCompute\t{2}\tModel\t{3}\tTraining\t{4}\tNumRecords\t{5}\tkey\t{6}",
        new Object[]{InetAddress.getLocalHost().getHostName(), finishTime - startTime, computeTime, modelOverhead,
            trainingOverhead, trainingData.size(), Arrays.toString(lrInput.getCachedKeys().toArray())});

    return new MultiClassGradientFunctionOutput(cumGradient, numPositive, trainingData.size(),
        startTime, modelLoadedTime, trainingLoadedTime, finishTime);
  }

  @Override
  public Codec<MultiClassGradientFunctionInput> getInputCodec() {
    return new MultiClassInputCodec();
  }

  @Override
  public Codec<MultiClassGradientFunctionOutput> getOutputCodec() {
    return new MultiClassOutputCodec();
  }
}