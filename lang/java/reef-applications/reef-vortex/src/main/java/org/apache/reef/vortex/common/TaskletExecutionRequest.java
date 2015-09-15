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
package org.apache.reef.vortex.common;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.vortex.api.VortexFunction;
import org.apache.reef.vortex.api.VortexInput;

import java.io.Serializable;

/**
 * Request to execute a tasklet.
 */
@Unstable
public final class TaskletExecutionRequest<TInput extends VortexInput, TOutput extends Serializable>
    implements VortexRequest {
  private final int taskletId;
  private final VortexFunction<TInput, TOutput> userFunction;
  private final TInput input;

  /**
   * @return the type of this VortexRequest.
   */
  @Override
  public RequestType getType() {
    return RequestType.ExecuteTasklet;
  }

  /**
   * Vortex Master -> Vortex Worker request to execute a tasklet.
   */
  public TaskletExecutionRequest(final int taskletId,
                                 final VortexFunction<TInput, TOutput> userFunction,
                                 final TInput input) {
    this.taskletId = taskletId;
    this.userFunction = userFunction;
    this.input = input;
  }

  /**
   * Execute the function using the input.
   */
  public TOutput execute() throws Exception {
    return userFunction.call(input);
  }

  /**
   * Get id of the tasklet.
   */
  public int getTaskletId() {
    return taskletId;
  }
}
