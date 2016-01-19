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

package org.apache.reef.vortex.examples.als;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.vortex.common.VortexParser;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class DataParser implements VortexParser<DataSet<LongWritable, Text>, List<IndexedVector>> {

  private static final Logger LOG = Logger.getLogger(DataParser.class.getName());

  @Override
  public List<IndexedVector> parse(final DataSet<LongWritable, Text> pairs) {
    int numRatings = 0;
    int numLines = 0;
    final List<IndexedVector> vectors = new LinkedList<>();
    for (final Pair<LongWritable, Text> keyValue : pairs) {
      numLines++;
      final String[] tokens = keyValue.getSecond().toString().trim().split(" ");
      final int vectorIndex = Integer.parseInt(tokens[0]);
      final List<Integer> ratingIndexList = new LinkedList<>();
      final List<Float> ratingList = new LinkedList<>();
      for (int i = 1; i < tokens.length; i++) {
        final String[] indexAndRating = tokens[i].split(":");
        ratingIndexList.add(Integer.parseInt(indexAndRating[0]));
        ratingList.add(Float.parseFloat(indexAndRating[1]));
      }

      numRatings += ratingList.size();
      final IndexedVector indexedVector = new IndexedVector(vectorIndex, ratingIndexList, ratingList);
      vectors.add(indexedVector);
    }

    LOG.log(Level.INFO, "# lines : {0}, # ratings : {1}", new Object[]{numLines, numRatings});

    return vectors;
  }
}
