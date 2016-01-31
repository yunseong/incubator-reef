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
import org.apache.reef.vortex.api.VortexAggregateException;
import org.apache.reef.vortex.api.VortexAggregateFunction;
import org.apache.reef.vortex.examples.lr.multi.output.MultiClassGradientFunctionOutput;
import org.apache.reef.vortex.examples.lr.multi.output.MultiClassOutputCodec;
import org.apache.reef.vortex.examples.lr.vector.DenseVector;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by yunseong on 1/31/16.
 */
public class MLRAggregationFunction implements VortexAggregateFunction<MultiClassGradientFunctionOutput> {
  private static final Logger LOG = Logger.getLogger(MLRAggregationFunction.class.getName());

  private static final MultiClassOutputCodec CODEC = new MultiClassOutputCodec();

  @Override
  public MultiClassGradientFunctionOutput call(final List<MultiClassGradientFunctionOutput> taskletOutputs)
      throws VortexAggregateException {
    final int numClasses = taskletOutputs.get(0).getPartialGradient().length;
    final int dimension = taskletOutputs.get(0).getPartialGradient()[0].getDimension();
    final DenseVector[] vectors = new DenseVector[numClasses];
    for (int i = 0; i < vectors.length; i++) {
      vectors[i] = new DenseVector(dimension);
    }
    int numPositive = 0;
    int numTotal = 0;
    for (int i = 0; i < taskletOutputs.size(); i++) {
      final MultiClassGradientFunctionOutput out = taskletOutputs.get(i);
      numPositive += out.getCountPositive();
      numTotal += out.getCountTotal();
      vectors[i].axpy(1.0f, out.getPartialGradient()[i]);
    }
    LOG.log(Level.INFO, "#Aggregate# {0}", taskletOutputs.size());
    return new MultiClassGradientFunctionOutput(vectors, numPositive, numTotal);
  }

  @Override
  public Codec<MultiClassGradientFunctionOutput> getOutputCodec() {
    return CODEC;
  }
}
