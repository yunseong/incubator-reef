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
public final class MapBasedVector implements Serializable {

  // Use Tree map for the sorted order.
  private HashMap<Integer, Float> map;
  private int dimension;

  private MapBasedVector() {
  }

  /**
   * Constructor of the Sparse Vector.
   **/
  public MapBasedVector(final int dimension) {
    this(new HashMap<Integer, Float>(), dimension);
  }

  /**
   * Constructor of the Sparse Vector.
   **/
  public MapBasedVector(final HashMap<Integer, Float> map, final int dimension) {
    this.map = map;
    this.dimension = dimension;
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
  public float dot(final MapBasedVector vector) {
    if (dimension != vector.dimension) {
      throw new RuntimeException("Error : Vector lengths are not equal");
    }

    float sum = 0.0f;

    final MapBasedVector smallVector = map.size() < vector.map.size() ? this : vector;
    final MapBasedVector largeVector = map.size() >= vector.map.size() ? this : vector;

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
  public MapBasedVector plus(final MapBasedVector vector) {
    if (this.dimension != vector.dimension) {
      throw new RuntimeException("Error : Vector lengths are not equal");
    }

    final MapBasedVector result = new MapBasedVector(dimension);

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
  public void addVector(final MapBasedVector vector) {
    for (final Map.Entry<Integer, Float> entry : vector.map.entrySet()) {
      final int index = entry.getKey();
      final float value = getValue(index) + entry.getValue();
      putValue(index, value);
    }
  }

  /**
   * Return v1 + c * v2 where v2 is the ArrayBasedVector.
   * @param coefficient
   * @param vector
   */
  public void addVector(final float coefficient, final ArrayBasedVector vector) {
    for (int i = 0; i < vector.getIndices().length; i++) {
      final int index = vector.getIndices()[i];
      final float value = getValue(index) + coefficient * vector.getValues()[i];
      putValue(index, value);
    }
  }

  /**
   * Multiply the n to all the elements.
   * @param n
   * @return
   */
  public MapBasedVector nTimes(final float n) {
    final MapBasedVector result = new MapBasedVector(dimension);
    for (final Map.Entry<Integer, Float> entry : map.entrySet()) {
      final int index = entry.getKey();
      final float value = n * entry.getValue();
      result.putValue(index, value);
    }
    return result;
  }

  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append('{');

    int count = 0;
    for (final Map.Entry<Integer, Float> entry : map.entrySet())  {
      builder.append(entry.getKey()).append(':').append(entry.getValue());
      if (++count < map.size()) {
        builder.append(", ");
      }
    }
    builder.append('}');

    builder.length();
    return builder.toString();
  }

  public static void main(final String[] args) {
    System.out.println("Sparse Vector Test\n");

    System.out.println("Enter size of sparse vectors");

    MapBasedVector v1 = new MapBasedVector(70000);
    MapBasedVector v2 = new MapBasedVector(70000);

    v1.putValue(3, 1.0f);
    v1.putValue(2500, 6.3f);
    v1.putValue(5000, 10.0f);
    v1.putValue(60000, -6.3f);

    v2.putValue(1, 7.5f);
    v2.putValue(3, 5.7f);
    v2.putValue(2500, -6.3f);

    System.out.println("\n");
    System.out.println("Vector v1 = " + v1);
    System.out.println("Vector v2 = " + v2);
    System.out.println("\nv1 dot v2 = " + v1.dot(v2));
    System.out.println("v1  +  v2   = " + v1.plus(v2));
    System.out.println("2 * v2   = " + v2.nTimes(2.0f));

    v1.addVector(v2);
    System.out.println("v1 = v1 + v2 = " + v1);

    MapBasedVector v3 = new MapBasedVector(10);
    MapBasedVector v4 = new MapBasedVector(10);
    v3.putValue(0, 10.0f);
    v3.putValue(1, 1.0f);
    v3.putValue(2, 3.0f);
    v3.putValue(9, 10.0f);

    v4.putValue(0, 0.0f);
    v4.putValue(1, 4.0f);
    v4.putValue(2, 10.0f);
    v4.putValue(9, 0.0f);

    System.out.println("Vector v3 = " + v3);
    System.out.println("Vector v4 = " + v4);
    System.out.println("v3 + v4 = " + v3.plus(v4));
    System.out.println("4 * v3 = " + v3.nTimes(4));
    System.out.println("v3 dot v4 = " + v3.dot(v4));
    System.out.println("v4 dot v3 = " + v4.dot(v3));
  }
}
