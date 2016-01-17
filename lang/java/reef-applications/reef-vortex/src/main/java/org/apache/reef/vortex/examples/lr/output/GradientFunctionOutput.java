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
package org.apache.reef.vortex.examples.lr.output;

import org.apache.reef.vortex.examples.lr.vector.DenseVector;

/**
 * Representation of partial result of the Logistic Regression in each iteration.
 */
public final class GradientFunctionOutput {
  private final DenseVector partialGradient;
  private int countPositive;
  private int countTotal;

  public GradientFunctionOutput(final DenseVector partialGradient, final int countPositive, final int countTotal) {
    this.partialGradient = partialGradient;
    this.countPositive = countPositive;
    this.countTotal = countTotal;
  }

  public DenseVector getPartialGradient() {
    return partialGradient;
  }

  public int getCountPositive() {
    return countPositive;
  }

  public int getCountTotal() {
    return countTotal;
  }
}
