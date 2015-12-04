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

import edu.snu.utils.SVector;
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
public final class RowParser
    implements VortexParser<DataSet<LongWritable, Text>, ArrayList<Row>> {
  private static final int MODEL_DIM = 3231961;
  @Override
  public ArrayList<Row> parse(final DataSet<LongWritable, Text> dataSet) throws ParseException {
    final ArrayList<Row> vectorsList = new ArrayList<>();
    try {
      for (final Pair<LongWritable, Text> keyValue : dataSet) {
        final String[] split = keyValue.getSecond().toString().split(" ");

        final int output = Integer.parseInt(split[0]);
        final int[] indices = new int[split.length - 1];
        final double[] values = new double[split.length - 1];

        for (int i = 1; i < split.length; i++) {
          final String[] column = split[i].split(":");
          final int index = Integer.parseInt(column[0]);
          final double value = Double.parseDouble(column[1]);

          indices[i-1] = index - 1;
          values[i-1] = value;
        }

        final SVector svector = new SVector(indices, values, MODEL_DIM + 1);
        svector.update(MODEL_DIM, 1); // Constant term?
        vectorsList.add(new Row(output, svector));
      }
      return vectorsList;
    } catch (final NumberFormatException e) {
      throw new ParseException(e.getMessage());
    }
  }
}


