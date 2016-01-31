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
package org.apache.reef.vortex.driver;

import net.jcip.annotations.NotThreadSafe;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.vortex.common.CachedDataResponse;
import org.apache.reef.vortex.api.VortexAggregateFunction;
import org.apache.reef.vortex.api.VortexAggregatePolicy;
import org.apache.reef.vortex.api.VortexFunction;
import org.apache.reef.vortex.common.TaskletAggregateExecutionRequest;
import org.apache.reef.vortex.common.TaskletAggregationRequest;
import org.apache.reef.vortex.common.TaskletCancellationRequest;
import org.apache.reef.vortex.common.TaskletExecutionRequest;

import java.util.*;

/**
 * Representation of a VortexWorkerManager in Driver.
 */
@NotThreadSafe
@DriverSide
class VortexWorkerManager {
  private final VortexRequestor vortexRequestor;
  private final RunningTask reefTask;
  private final HashMap<Integer, Tasklet> runningTasklets = new HashMap<>();

  VortexWorkerManager(final VortexRequestor vortexRequestor, final RunningTask reefTask) {
    this.vortexRequestor = vortexRequestor;
    this.reefTask = reefTask;
  }

  /**
   * Sends an {@link VortexAggregateFunction} and its {@link VortexFunction} to a
   * {@link org.apache.reef.vortex.evaluator.VortexWorker}.
   */
  <TInput, TOutput> void sendAggregateFunction(final int aggregateFunctionId,
                                               final VortexAggregateFunction<TOutput> aggregateFunction,
                                               final VortexFunction<TInput, TOutput> function,
                                               final VortexAggregatePolicy policy) {
    final TaskletAggregationRequest<TInput, TOutput> taskletAggregationRequest =
        new TaskletAggregationRequest<>(aggregateFunctionId, aggregateFunction, function, policy);

    // The send is synchronous such that we make sure that the aggregate function is sent to the
    // target worker before attempting to launch an aggregateable tasklet on it.
    vortexRequestor.send(reefTask, taskletAggregationRequest);
  }


  /**
   * Sends a request to launch a Tasklet on a {@link org.apache.reef.vortex.evaluator.VortexWorker}.
   */
  <TInput, TOutput> void launchTasklet(final Tasklet<TInput, TOutput> tasklet) {
    assert !runningTasklets.containsKey(tasklet.getId());
    runningTasklets.put(tasklet.getId(), tasklet);

    if (tasklet.getAggregateFunctionId().isPresent()) {
      // function is aggregateable.
      final TaskletAggregateExecutionRequest<TInput> taskletAggregateExecutionRequest =
          new TaskletAggregateExecutionRequest<>(tasklet.getId(), tasklet.getAggregateFunctionId().get(),
              tasklet.getInput());
      vortexRequestor.sendAsync(reefTask, taskletAggregateExecutionRequest);
    } else {
      // function is not aggregateable.
      final TaskletExecutionRequest<TInput, TOutput> taskletExecutionRequest
          = new TaskletExecutionRequest<>(tasklet.getId(), tasklet.getUserFunction(), tasklet.getInput());
      vortexRequestor.sendAsync(reefTask, taskletExecutionRequest);
    }
  }

  /**
   * Sends a request to cancel a Tasklet on a {@link org.apache.reef.vortex.evaluator.VortexWorker}.
   */
  void cancelTasklet(final int taskletId) {
    final TaskletCancellationRequest cancellationRequest = new TaskletCancellationRequest(taskletId);
    vortexRequestor.sendAsync(reefTask, cancellationRequest);
  }

  void sendCacheData(final CachedDataResponse cachedDataResponse) {
    vortexRequestor.send(reefTask, cachedDataResponse);
  }

  void sendCacheData(final byte[] serizliedCachedDataResponse) {
    vortexRequestor.send(reefTask, serizliedCachedDataResponse);
  }

  List<Tasklet> taskletsDone(final List<Integer> taskletIds) {
    final List<Tasklet> taskletList = new ArrayList<>();
    for (final int taskletId : taskletIds) {
      taskletList.add(runningTasklets.remove(taskletId));
    }

    return Collections.unmodifiableList(taskletList);
  }

  Collection<Tasklet> removed() {
    return runningTasklets.isEmpty() ? null : runningTasklets.values();
  }

  void terminate() {
    reefTask.close();
  }

  String getId() {
    return reefTask.getId();
  }

  /**
   * @return the description of this worker in string.
   */
  @Override
  public String toString() {
    return "VortexWorkerManager: " + getId();
  }

  /**
   * For unit tests only.
   */
  boolean containsTasklet(final Integer taskletId) {
    return runningTasklets.containsKey(taskletId);
  }
}
