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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sparse Vector based on Array.
 */
public final class ArrayBasedVector implements Serializable {
  private float[] values;
  private int[] indices;
  private int output;

  private ArrayBasedVector() {
  }

  public ArrayBasedVector(final float[] values, final int[] indices, final int output) {
    this.values = values;
    this.indices = indices;
    this.output = output;

    if (values.length != indices.length) {
      Logger.getLogger(ArrayBasedVector.class.getName())
          .log(Level.WARNING, "The length does not match: indices {0} / values {1]",
              new Object[]{indices.length, values.length});
    }
  }

  public int getOutput() {
    return output;
  }

  public int[] getIndices() {
    return indices;
  }

  public float[] getValues() {
    return values;
  }
}
