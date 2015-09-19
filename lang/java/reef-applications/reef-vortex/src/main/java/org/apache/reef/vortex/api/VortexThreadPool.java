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
package org.apache.reef.vortex.api;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.vortex.driver.VortexMaster;

import javax.inject.Inject;
import java.io.Serializable;

/**
 * Distributed thread pool.
 */
@Unstable
public final class VortexThreadPool {
  private final VortexMaster vortexMaster;

  @Inject
  private VortexThreadPool(final VortexMaster vortexMaster) {
    this.vortexMaster = vortexMaster;
  }

  /**
   * @param function to run on Vortex
   * @param input of the function
   * @param <TInput> input type
   * @param <TOutput> output type
   * @return VortexFuture for tracking execution progress
   */
  public <TInput extends VortexInput, TOutput extends Serializable> VortexFuture<TOutput>
      submit(final VortexFunction<TInput, TOutput> function, final TInput input) {
    return vortexMaster.enqueueTasklet(function, input);
  }
}