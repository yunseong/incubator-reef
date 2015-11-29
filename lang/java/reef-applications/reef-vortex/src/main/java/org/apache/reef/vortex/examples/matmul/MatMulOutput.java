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
package org.apache.reef.vortex.examples.matmul;

import java.io.Serializable;

/**
 * Output of {@link MatMulFunction} which contains the sub-matrix and index of it in the entire result.
 */
class MatMulOutput implements Serializable {
  private final int index;
  private final Matrix<Double> result;

  /**
   * Constructor of the output.
   * @param index Index of the sub-matrix in the entire result.
   * @param result Result of multiplication (sub-matrix).
   */
  MatMulOutput(final int index, final Matrix<Double> result) {
    this.index = index;
    this.result = result;
  }

  /**
   * @return Index of the sub-matrix in the entire matrix.
   */
  int getIndex() {
    return index;
  }

  /**
   * @return Result of multiplication (sub-matrix).
   */
  Matrix<Double> getResult() {
    return result;
  }
}
