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

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by v-yunlee on 10/16/2015.
 */
public final class ArrayBasedVector {
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

  public HashMap<Integer, Float> toHashMap(final int modelDim) {
    final HashMap<Integer, Float> map = new HashMap<>(values.length);

    for (int i = 0; i < values.length; i++) {
      // Just for partial test.
      if (indices[i] <= modelDim) {
        map.put(indices[i], values[i]);
      }
    }
    return map;
  }

  public int getOutput() {
    return output;
  }
}
