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

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.vortex.api.VortexAggregateFunction;
import org.apache.reef.vortex.api.VortexAggregatePolicy;
import org.apache.reef.vortex.api.VortexFunction;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A repository for {@link VortexAggregateFunction} and its associated {@link VortexFunction},
 * used to pass functions between VortexMaster and RunningWorkers, as well as used to cache functions
 * for VortexWorkers on AggregateRequests and AggregateExecutionRequests.
 */
@ThreadSafe
@Unstable
@Private
public final class AggregateFunctionRepository {
  private final ConcurrentMap<Integer, Triple<VortexAggregateFunction, VortexFunction, VortexAggregatePolicy>>
      aggregateFunctionMap = new ConcurrentHashMap<>();

  @Inject
  private AggregateFunctionRepository() {
  }

  /**
   * Associates an aggregate function ID with a {@link VortexAggregateFunction} and a {@link VortexFunction}.
   */
  public Triple<VortexAggregateFunction, VortexFunction, VortexAggregatePolicy> put(
      final int aggregateFunctionId,
      final VortexAggregateFunction aggregateFunction,
      final VortexFunction function,
      final VortexAggregatePolicy policy) {
    return aggregateFunctionMap.put(aggregateFunctionId, new ImmutableTriple<>(aggregateFunction, function, policy));
  }

  /**
   * Gets the {@link VortexAggregateFunction} associated with the aggregate function ID.
   */
  public VortexAggregateFunction getAggregateFunction(final int aggregateFunctionId) {
    return aggregateFunctionMap.get(aggregateFunctionId).getLeft();
  }

  /**
   * Gets the {@link VortexFunction} associated with the aggregate function ID.
   */
  public VortexFunction getFunction(final int aggregateFunctionId) {
    return aggregateFunctionMap.get(aggregateFunctionId).getMiddle();
  }

  /**
   * Gets the {@link VortexAggregatePolicy} associated with the aggregate function ID.
   */
  public VortexAggregatePolicy getPolicy(final int aggregateFunctionId) {
    return aggregateFunctionMap.get(aggregateFunctionId).getRight();
  }
}