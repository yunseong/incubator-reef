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

import org.apache.reef.vortex.examples.lr.input.MapBasedVector;

import java.io.Serializable;

/**
 * Representation of partial result of the Logistic Regression in each iteration.
 */
public final class PartialResult implements Serializable {
  private final MapBasedVector partialGradient;
  private int numPositive;
  private int numNegative;
  private int count;

  public PartialResult(final MapBasedVector partialGradient,
                       final int numPositive,
                       final int numNegative) {
    this.partialGradient = partialGradient;
    this.numPositive = numPositive;
    this.numNegative = numNegative;
    this.count = numNegative + numPositive;
  }

  public void addResult(final PartialResult result) {
    partialGradient.addVector(result.getPartialGradient());
    numPositive += result.getNumPositive();
    numNegative += result.getNumNegative();
    count += result.getCount();
  }

  public MapBasedVector getPartialGradient() {
    return partialGradient;
  }

  public int getNumPositive() {
    return numPositive;
  }

  public int getNumNegative() {
    return numNegative;
  }

  public int getCount() {
    return count;
  }

  @Override
  public String toString() {
    return "PartialResult{" +
        "count=" + count +
        ", partialGradient=" + partialGradient +
        ", numPositive=" + numPositive +
        ", numNegative=" + numNegative +
        '}';
  }
}
