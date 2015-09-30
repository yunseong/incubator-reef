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
package org.apache.reef.vortex.examples.lr.input;

import java.io.Serializable;

/**
 * The representation of a training data.
 */
public final class Row implements Serializable {
  private final SparseVector vector;
  private final int output;

  private Row(final SparseVector vector, final int output) {
    this.vector = vector;
    this.output = output;
  }

  /**
   * Create a new instance of a training data.
   *
   * @param vector
   * @param output
   * @return
   */
  public static Row getInstance(final SparseVector vector, final int output) {
    return new Row(vector, output);
  }

  public int getDimension() {
    return vector.getDimension();
  }

  /**
   * @return The feature vector.
   */
  public SparseVector getFeature() {
    return vector;
  }

  /**
   * @return The output which indicates {@code true} or {@code false}.
   */
  public int getOutput() {
    return output;
  }
}
