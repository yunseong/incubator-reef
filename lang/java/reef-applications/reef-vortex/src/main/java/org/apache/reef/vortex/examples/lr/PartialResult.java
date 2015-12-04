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

import org.apache.reef.vortex.examples.lr.input.DenseVector;

import java.io.Serializable;

/**
 * Representation of partial result of the Logistic Regression in each iteration.
 */
public final class PartialResult implements Serializable {
  private final DenseVector cumGradient;
  private double loss;
  private int numPositive;
  private int count;

  public PartialResult(final DenseVector cumGradient) {
    this.cumGradient = cumGradient;
    this.loss = 0.0f;
    this.numPositive = 0;
    this.count = 0;
  }

  public void addResult(final DenseVector cumGradient,
                        final double loss,
                        final boolean isPositive) {
    this.cumGradient.set(cumGradient);
    this.loss += loss;
    count++;
    if (isPositive) {
      numPositive++;
    }
  }

  public DenseVector getCumGradient() {
    return cumGradient;
  }

  public float getLoss() {
    return (float) loss;
  }

  public int getNumPositive() {
    return numPositive;
  }

  public int getCount() {
    return count;
  }
}
