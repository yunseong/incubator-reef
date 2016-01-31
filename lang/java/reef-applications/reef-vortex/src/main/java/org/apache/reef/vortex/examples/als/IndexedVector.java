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

import java.util.List;

/**
 * Created by kgw on 2016. 1. 7..
 */
public final class IndexedVector {

  private IndexedVector() {
  }

  private int[] ratingIndexes;
  private float[] ratings;
  private int index;

  public IndexedVector(final int index, final List<Integer> ratingIndexList, final List<Float> ratingList) {
    this.ratingIndexes = new int[ratingIndexList.size()];
    this.ratings = new float[ratingList.size()];

    int j = 0;
    for (final int ratingIndex : ratingIndexList) {
      ratingIndexes[j++] = ratingIndex;
    }

    j = 0;
    for (final float rating : ratingList) {
      ratings[j++] = rating;
    }

    this.index = index;
  }

  public int getRatingIndex(final int pos) {
    return ratingIndexes[pos];
  }

  public float getRating(final int pos) {
    return ratings[pos];
  }

  public int getIndex() {
    return index;
  }

  public int size() {
    return ratings.length;
  }

  public String toString() {
    final StringBuilder builder = new StringBuilder()
        .append(index);

    for (int i = 0; i < ratings.length; i++) {
      builder.append(" ").append(ratingIndexes[i]).append(":").append(ratings[i]);
    }

    return builder.toString();
  }
}