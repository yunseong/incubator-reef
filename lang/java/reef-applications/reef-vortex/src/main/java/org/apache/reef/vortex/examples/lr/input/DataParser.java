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

/**
 * Parse concatenated lines into Training Data.
 */
public final class DataParser {
  private DataParser() {
  }

  static TrainingData parseTrainingData(final String str, final int modelDim) throws ParseException {
    final TrainingData result = new TrainingData();
    final String[] split = str.split("#");
    for (final String parsed : split) {
      if (null != parsed && parsed.length() != 0) {
        result.addRow(parseLine(parsed, modelDim));
      }
    }
    return result;
  }

  /**
   * Parse a line and create a training data.
   */
  private static Row parseLine(final String line, final int modelDim) throws ParseException {
    final SparseVector feature = new SparseVector(modelDim);

    final String[] split = line.split(" ");

    try {
      final int output = Integer.valueOf(split[0]);

      for (int i = 1; i < split.length; i++) {
        final String[] column = split[i].split(":");

        final int index = Integer.valueOf(column[0]);
        final double value = Double.valueOf(column[1]);

        if (index >= modelDim) {
          // Restrict the dimension of model to save our time.
          break;
        }
        feature.putValue(index, value);
      }
      return Row.getInstance(feature, output);

    } catch (final NumberFormatException e) {
      throw new ParseException(e.getMessage());
    }
  }
}
