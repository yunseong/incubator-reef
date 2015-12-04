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
public final class SparseVector implements Serializable {

  // Use Tree map for the sorted order.
  private HashMap<Integer, Float> map;
  private int dimension;

  public SparseVector() {
  }

  /**
   * Constructor of the Sparse Vector.
   **/
  public SparseVector(final int dimension) {
    this(new HashMap<Integer, Float>(), dimension);
  }

  /**
   * Constructor of the Sparse Vector.
   **/
  public SparseVector(final HashMap<Integer, Float> map, final int dimension) {
    this.map = map;
    this.dimension = dimension;
  }

  public void set(final SparseVector vec) {
    this.map = vec.map;
    this.dimension = vec.dimension;
  }
  /**
   * Insert a value to the index.
   **/
  public void putValue(final int index, final float value) {
    if (index < 0 || index > dimension) {
      throw new RuntimeException("Index out of bounds:" + index + "/" + dimension);
    }

    // If the value is 0.0, then delete the value if exists.
    if (value == 0.0) {
      map.remove(index);
    } else {
      map.put(index, value);
    }
  }

  /**
   * @param index Index of the element.
   * @return Get the value at the index.
   */
  public float getValue(final int index) {
    if (index < 0 || index > dimension) {
      throw new RuntimeException("Index out of bounds:" + index + "/" + dimension);
    }

    if (map.containsKey(index)) {
      return map.get(index);
    } else {
      return 0.0f;
    }
  }

  /**
   * Get the getDimension of the vector.
   * @return
   */
  public int getDimension() {
    return dimension;
  }

  /**
   * Compute the Inner product.
   * @param vector The other vector.
   * @return The value of inner product.
   */
  public float dot(final SparseVector vector) {
    if (dimension != vector.dimension) {
      throw new RuntimeException("Error : Vector lengths are not equal");
    }

    float sum = 0.0f;

    final SparseVector smallVector = map.size() < vector.map.size() ? this : vector;
    final SparseVector largeVector = map.size() >= vector.map.size() ? this : vector;

    for (final Map.Entry<Integer, Float> entry : smallVector.map.entrySet()) {
      if (largeVector.map.containsKey(entry.getKey())) {
        final int index = entry.getKey();
        sum += entry.getValue() * largeVector.getValue(index);
      }
    }
    return sum;
  }

  public float dot(final ArrayBasedVector vector) {
    float sum = 0.0f;

    for (int i = 0; i < vector.getIndices().length; i++) {
      final int index = vector.getIndices()[i];
      if (map.containsKey(index)) {
        sum += map.get(index) * vector.getValues()[i];
      }
    }
    return sum;
  }

  /**
   * Compute the sum of the two vectors. They should have the same dimension.
   * @param vector
   * @return
   */
  public SparseVector plus(final SparseVector vector) {
    if (this.dimension != vector.dimension) {
      throw new RuntimeException("Error : Vector lengths are not equal");
    }

    final SparseVector result = new SparseVector(dimension);

    for (final Map.Entry<Integer, Float> entry : this.map.entrySet()) {
      result.putValue(entry.getKey(), entry.getValue());
    }

    for (final Map.Entry<Integer, Float> entry : vector.map.entrySet()) {
      final int index = entry.getKey();
      final float originalValue = result.getValue(index);
      result.putValue(entry.getKey(), originalValue + entry.getValue());
    }
    return result;
  }

  /**
   * In-place addition of two vectors.
   * @param vector
   */
  public void axpy(final double coefficient, final SparseVector vector) {
    for (final Map.Entry<Integer, Float> entry : vector.map.entrySet()) {
      final int index = entry.getKey();
      final double value = getValue(index) + coefficient * entry.getValue();
      putValue(index, (float) value);
    }
  }

  /**
   * Return v1 + c * v2 where v2 is the ArrayBasedVector.
   * @param coefficient
   * @param vector
   */
  public void axpy(final double coefficient, final ArrayBasedVector vector) {
    for (int i = 0; i < vector.getIndices().length; i++) {
      final int index = vector.getIndices()[i];
      final double value = getValue(index) + coefficient * vector.getValues()[i];
      putValue(index, (float) value);
    }
  }

  /**
   * In-place scale multiplyting n to all the elements.
   * @param n
   * @return
   */
  public void nTimes(final double n) {
    for (final Map.Entry<Integer, Float> entry : map.entrySet()) {
      final int index = entry.getKey();
      final float value = (float)(n * entry.getValue());
      putValue(index, value);
    }
  }
}
