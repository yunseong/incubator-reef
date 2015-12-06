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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.vortex.common.VortexParser;

import java.util.ArrayList;


/**
 * Parses DataSet which was read by RecordReader to Vector list.
 * To be Serializable, made the output type ArrayList.
 */
public final class ArrayBasedVectorParser
    implements VortexParser<DataSet<LongWritable, Text>, ArrayList<ArrayBasedVector>> {

  @Override
  public ArrayList<ArrayBasedVector> parse(final DataSet<LongWritable, Text> dataSet) throws ParseException {
    final ArrayList<ArrayBasedVector> vectorsList = new ArrayList<>();
    try {
      for (final Pair<LongWritable, Text> keyValue : dataSet) {
        final String[] split = keyValue.getSecond().toString().split(" ");

        final int output = Integer.valueOf(split[0]);
        final int[] indices = new int[split.length - 1];
        final float[] values = new float[split.length - 1];

        for (int i = 1; i < split.length; i++) {
          final String[] column = split[i].split(":");
          final int index = Integer.valueOf(column[0]);
          final float value = Float.valueOf(column[1]);

          indices[i-1] = index;
          values[i-1] = value;
        }
        vectorsList.add(new ArrayBasedVector(values, indices, output));
      }
      return vectorsList;
    } catch (final NumberFormatException e) {
      throw new ParseException(e.getMessage());
    }
  }
}


