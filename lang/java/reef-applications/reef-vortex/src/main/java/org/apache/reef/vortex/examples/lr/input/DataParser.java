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

import java.util.ArrayList;

/**
 * Parse concatenated lines into Training Data.
 */
public final class DataParser {
  private DataParser() {
  }

  static TrainingData parseTrainingData(final ArrayList<ArrayBasedVector> partitions, final int modelDim) {
    final TrainingData result = new TrainingData();
    for (final ArrayBasedVector partition : partitions) {
      result.addRow(Row.getInstance(new SparseVector(partition.toHashMap(modelDim), modelDim), partition.getOutput()));
    }
    return result;
  }
}
