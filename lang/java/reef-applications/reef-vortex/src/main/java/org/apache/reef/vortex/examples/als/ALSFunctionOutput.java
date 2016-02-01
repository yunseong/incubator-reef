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

/**
 * Created by kgw on 2016. 2. 2..
 */
public class ALSFunctionOutput {

  public ResultVector[] resultVectors;
  public long launched;
  public long modelLoaded;
  public long trainingLoaded;
  public long computeFinished;

  public ALSFunctionOutput() {
  }

  public ALSFunctionOutput(final ResultVector[] resultVectors,
                           final long launched,
                           final long modelLoaded,
                           final long trainingLoaded,
                           final long computeFinished) {
    this.resultVectors = resultVectors;
    this.launched = launched;
    this.modelLoaded = modelLoaded;
    this.trainingLoaded = trainingLoaded;
    this.computeFinished = computeFinished;
  }
}
