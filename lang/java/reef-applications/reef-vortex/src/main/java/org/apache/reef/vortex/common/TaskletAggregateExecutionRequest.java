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
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;

/**
 * A request from the Vortex Driver to run an aggregate-able function.
 */
@Unstable
@Private
@DriverSide
public final class TaskletAggregateExecutionRequest<TInput> implements VortexRequest {
  private final TInput input;
  private final int aggregateFunctionId;
  private final int taskletId;

  public TaskletAggregateExecutionRequest(final int taskletId,
                                          final int aggregateFunctionId,
                                          final TInput input) {
    this.taskletId = taskletId;
    this.input = input;
    this.aggregateFunctionId = aggregateFunctionId;
  }

  /**
   * @return input of the request.
   */
  public TInput getInput() {
    return input;
  }

  /**
   * @return tasklet ID corresponding to the tasklet request.
   */
  public int getTaskletId() {
    return taskletId;
  }

  /**
   * @return the AggregateFunctionID of the request.
   */
  public int getAggregateFunctionId() {
    return aggregateFunctionId;
  }

  @Override
  public RequestType getType() {
    return RequestType.ExecuteAggregateTasklet;
  }
}
