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

import java.io.Serializable;

/**
 * Created by kgw on 2016. 1. 11..
 */
public class ResultVector implements Serializable {
  private final int index;
  private final int numRatings;
  private final double sumSquaredError;
  private final double[] vector;

  public ResultVector(final int index, final int numRatings, final double sumSquaredError, final double[] vector) {
    this.index = index;
    this.numRatings = numRatings;
    this.sumSquaredError = sumSquaredError;
    this.vector = vector;
  }

  public int getIndex() {
    return index;
  }

  public int getNumRatings() {
    return numRatings;
  }

  public double getSumSquaredError() {
    return sumSquaredError;
  }

  public double[] getVector() {
    return vector;
  }
}
