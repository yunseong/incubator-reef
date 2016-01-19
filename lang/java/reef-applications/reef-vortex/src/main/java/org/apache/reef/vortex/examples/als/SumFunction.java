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

package org.apache.reef.vortex.examples.als;

import org.apache.reef.io.serialization.Codec;
import org.apache.reef.vortex.api.VortexFunction;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class SumFunction implements VortexFunction<SumFunctionInput, float[]> {

  private static final Logger LOG = Logger.getLogger(SumFunction.class.getName());

  private int numItems;

  private SumFunction() {
  }

  public SumFunction(final int numItems) {
    this.numItems = numItems;
  }

  private float[] getSumVector(final List<IndexedVector> indexedVectors) {
    final float[] sumVector = new float[numItems];
    for (final IndexedVector indexedVector : indexedVectors) {
      for (int i = 0; i < indexedVector.size(); i++) {
        sumVector[indexedVector.getRatingIndex(i)] += indexedVector.getRating(i);
      }
    }

    return sumVector;
  }

  @Override
  public float[] call(final SumFunctionInput input) throws Exception {
    final Runtime r = Runtime.getRuntime();
    final long startTime = System.currentTimeMillis();
    final long startMemory = (r.totalMemory() - r.freeMemory())/1048576;

    final long modelLoadedTime = System.currentTimeMillis();
    final long modelLoadedMemory = (r.totalMemory() - r.freeMemory())/1048576;

    final List<IndexedVector> trainingData = input.getUserVectors();
    final long trainingLoadedTime = System.currentTimeMillis();
    final long trainingLoadedMemory = (r.totalMemory() - r.freeMemory())/1048576;

    LOG.log(Level.INFO, "!SUM!Init\t{0}\tUsed\t{1}->{2}->{3}\tMax\t{4}\tTotal\t{5}",
        new Object[] {InetAddress.getLocalHost().getHostName(),
            startMemory, modelLoadedMemory, trainingLoadedMemory, r.maxMemory()/1048576, r.totalMemory()/1048576});

    final float[] sum = getSumVector(trainingData);

    final long finishTime = System.currentTimeMillis();
    final long executionTime = finishTime - trainingLoadedTime;
    final long modelOverhead = modelLoadedTime - startTime;
    final long trainingOverhead = trainingLoadedTime - modelLoadedTime;

    LOG.log(Level.INFO, "!SUM!\t{0}\tUsed\t{1}->{2}\tMax\t{3}\tTotal\t{4}" +
            "\tExecution\t{5}\tModel\t{6}\tTraining\t{7}\tRowNum\t{8}\tkey\t{9}",
        new Object[]{
            InetAddress.getLocalHost().getHostName(), startMemory, (r.totalMemory() - r.freeMemory())/1048576,
            r.maxMemory()/1048576, r.totalMemory()/1048576,
            executionTime, modelOverhead, trainingOverhead, trainingData.size(),
            Arrays.toString(input.getCachedKeys().toArray())});


    return sum;
  }

  @Override
  public Codec<SumFunctionInput> getInputCodec() {
    return new SumFunctionInputCodec();
  }

  @Override
  public Codec<float[]> getOutputCodec() {
    return new SumFunctionOutputCodec();
  }
}
