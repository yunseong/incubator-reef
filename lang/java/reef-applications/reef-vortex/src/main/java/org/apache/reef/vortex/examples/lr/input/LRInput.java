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

/**
 * Input used in Logistic Regression which does not cache the data.
 */
public class LRInput implements Serializable {
  private StringBuilder trainingData;
  private SparseVector parameterVector;
  private final int modelDim;

  public LRInput(final SparseVector parameterVector,
                 final StringBuilder trainingData,
                 final int modelDim) {
    this.parameterVector = parameterVector;
    this.trainingData = trainingData;
    this.modelDim = modelDim;
  }

  public SparseVector getParameterVector() {
    return parameterVector;
  }

  public TrainingData getTrainingData() throws ParseException {
    return DataParser.parseTrainingData(trainingData.toString(), modelDim);
  }
}
