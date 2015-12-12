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
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of a sparse vector based on the tree map.
 */
public final class DenseVector implements Serializable {
  private float[] data;

  private DenseVector() {
  }

  public DenseVector(final int dimension) {
    this.data = new float[dimension];
  }

  /**
   * Constructor of the Sparse Vector.
   **/
  public DenseVector(final float[] data) {
    this.data = data;
  }

  public float[] getData() {
    return data;
  }

  public int getDimension() {
    return data.length;
  }

  public double dot(final SparseVector vec) {
    double result = 0;
    for (int i = 0; i < vec.getIndices().length; i++) {
      final int index = vec.getIndices()[i];
      final float value = vec.getValues()[i];
      result += value * data[index];
    }
    return result;
  }

  public void axpy(final float a, final SparseVector x) {
    if (a == 1.0) {
      add(x);
    } else {
      for (int i = 0; i < x.getIndices().length; i++) {
        final int index = x.getIndices()[i];
        final float value = x.getValues()[i];
        data[index] += a * value;
      }
    }
  }

  public void axpy(final float a, final DenseVector x) {
    if (a == 1.0) {
      add(x);
    } else {
      for (int i = 0; i < x.getData().length; i++) {
        data[i] += (float) a * x.getData()[i];
      }
    }
  }

  public void add(final SparseVector vec) {
    for (int i = 0; i < vec.getIndices().length; i++) {
      final int index = vec.getIndices()[i];
      final double value = vec.getValues()[i];
      data[index] += (float) value;
    }
  }

  public void add(final DenseVector vec) {
    for (int i = 0; i < vec.getData().length; i++) {
      data[i] += vec.getData()[i];
    }
  }
}
