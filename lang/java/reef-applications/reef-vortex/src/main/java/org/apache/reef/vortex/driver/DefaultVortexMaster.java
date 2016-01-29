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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import net.jcip.annotations.ThreadSafe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.data.loading.impl.JobConfExternalConstructor;
import org.apache.reef.io.data.loading.impl.WritableSerializer;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.apache.reef.vortex.api.FutureCallback;
import org.apache.reef.vortex.api.VortexFunction;
import org.apache.reef.vortex.api.VortexFuture;
import org.apache.reef.vortex.common.*;
import org.apache.reef.vortex.common.exceptions.VortexCacheException;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default implementation of VortexMaster.
 * Uses two thread-safe data structures(pendingTasklets, runningWorkers) in implementing VortexMaster interface.
 */
@ThreadSafe
@DriverSide
final class DefaultVortexMaster implements VortexMaster {
  private static final Logger LOG = Logger.getLogger(DefaultVortexMaster.class.getName());
  private final Map<Integer, VortexFutureDelegate> taskletFutureMap = new HashMap<>();
  private final AtomicInteger taskletIdCounter = new AtomicInteger();
  private final RunningWorkers runningWorkers;
  private final PendingTasklets pendingTasklets;
  private final Executor executor;
  private final Cache<String, byte[]> cacheMap = CacheBuilder.newBuilder().softValues().build();


  /**
   * @param runningWorkers for managing all running workers.
   */
  @Inject
  DefaultVortexMaster(final RunningWorkers runningWorkers,
                      final PendingTasklets pendingTasklets,
                      @Parameter(VortexMasterConf.CallbackThreadPoolSize.class) final int threadPoolSize) {
    this.executor = Executors.newFixedThreadPool(threadPoolSize);
    this.runningWorkers = runningWorkers;
    this.pendingTasklets = pendingTasklets;
  }

  /**
   * Add a new tasklet to pendingTasklets.
   */
  @Override
  public <TInput, TOutput> VortexFuture<TOutput>
      enqueueTasklet(final VortexFunction<TInput, TOutput> function, final TInput input,
                     final Optional<FutureCallback<TOutput>> callback) {
    // TODO[REEF-500]: Simple duplicate Vortex Tasklet launch.
    final VortexFuture<TOutput> vortexFuture;
    final int id = taskletIdCounter.getAndIncrement();
    final Codec<TOutput> outputCodec = function.getOutputCodec();
    if (callback.isPresent()) {
      vortexFuture = new VortexFuture<>(executor, this, id, outputCodec, callback.get());
    } else {
      vortexFuture = new VortexFuture<>(executor, this, id, outputCodec);
    }

    final Tasklet tasklet = new Tasklet<>(id, function, input, vortexFuture);
    putDelegate(Collections.singletonList(tasklet), vortexFuture);
    this.pendingTasklets.addLast(tasklet);

    return vortexFuture;
  }

  /**
   * Cancels tasklets on the running workers.
   */
  @Override
  public void cancelTasklet(final boolean mayInterruptIfRunning, final int taskletId) {
    this.runningWorkers.cancelTasklet(mayInterruptIfRunning, taskletId);
  }

  /**
   * Add a new worker to runningWorkers.
   */
  @Override
  public void workerAllocated(final VortexWorkerManager vortexWorkerManager) {
    runningWorkers.addWorker(vortexWorkerManager);
  }

  /**
   * Remove the worker from runningWorkers and add back the lost tasklets to pendingTasklets.
   */
  @Override
  public void workerPreempted(final String id) {
    final Optional<Collection<Tasklet>> preemptedTasklets = runningWorkers.removeWorker(id);
    if (preemptedTasklets.isPresent()) {
      LOG.log(Level.INFO, "#Re# {0}", preemptedTasklets.get().size());
      for (final Tasklet tasklet : preemptedTasklets.get()) {
        LOG.log(Level.INFO, "#Re_id# {0}", tasklet.getId());
        pendingTasklets.addFirst(tasklet);
      }
    }
  }

  @Override
  public void workerReported(final String workerId, final WorkerReport workerReport) {
    for (final TaskletReport taskletReport : workerReport.getTaskletReports()) {
      switch (taskletReport.getType()) {
      case TaskletResult:
        final TaskletResultReport taskletResultReport = (TaskletResultReport) taskletReport;

        final int resultTaskletId = taskletResultReport.getTaskletId();
        final List<Integer> singletonResultTaskletId = Collections.singletonList(resultTaskletId);
        runningWorkers.doneTasklets(workerId, singletonResultTaskletId);
        fetchDelegate(singletonResultTaskletId).completed(resultTaskletId, taskletResultReport.getSerializedResult());

        break;
      case TaskletAggregationResult:
        final TaskletAggregationResultReport taskletAggregationResultReport =
            (TaskletAggregationResultReport) taskletReport;

        final List<Integer> aggregatedTaskletIds = taskletAggregationResultReport.getTaskletIds();
        runningWorkers.doneTasklets(workerId, aggregatedTaskletIds);
        fetchDelegate(aggregatedTaskletIds).aggregationCompleted(
            aggregatedTaskletIds, taskletAggregationResultReport.getSerializedResult());

        break;
      case TaskletCancelled:
        final TaskletCancelledReport taskletCancelledReport = (TaskletCancelledReport) taskletReport;
        final List<Integer> cancelledIdToList = Collections.singletonList(taskletCancelledReport.getTaskletId());
        runningWorkers.doneTasklets(workerId, cancelledIdToList);
        fetchDelegate(cancelledIdToList).cancelled(taskletCancelledReport.getTaskletId());

        break;
      case TaskletFailure:
        final TaskletFailureReport taskletFailureReport = (TaskletFailureReport) taskletReport;

        final int failureTaskletId = taskletFailureReport.getTaskletId();
        final List<Integer> singletonFailedTaskletId = Collections.singletonList(failureTaskletId);
        runningWorkers.doneTasklets(workerId, singletonFailedTaskletId);
        fetchDelegate(singletonFailedTaskletId).threwException(failureTaskletId, taskletFailureReport.getException());

        break;
      case TaskletAggregationFailure:
        final TaskletAggregationFailureReport taskletAggregationFailureReport =
            (TaskletAggregationFailureReport) taskletReport;

        final List<Integer> aggregationFailedTaskletIds = taskletAggregationFailureReport.getTaskletIds();
        runningWorkers.doneTasklets(workerId, aggregationFailedTaskletIds);
        fetchDelegate(aggregationFailedTaskletIds).aggregationThrewException(aggregationFailedTaskletIds,
            taskletAggregationFailureReport.getException());
        break;
      default:
        throw new RuntimeException("Unknown Report");
      }
    }

    if (workerReport.getCachedDataRequest() != null) {
      try {
        dataRequested(workerId, workerReport.getCachedDataRequest().getKeyId());
      } catch (final VortexCacheException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Terminate the job.
   */
  @Override
  public synchronized void terminate() {
    try {
      this.wait(1000);
    } catch (Exception e) {
      throw new RuntimeException("sleep interrupted");
    }
    runningWorkers.terminate();
  }

  /**
   * Puts a delegate to associate with a Tasklet.
   */
  private synchronized void putDelegate(final List<Tasklet> tasklets, final VortexFutureDelegate delegate) {
    for (final Tasklet tasklet : tasklets) {
      taskletFutureMap.put(tasklet.getId(), delegate);
    }
  }

  /**
   * Fetches a delegate that maps to the list of Tasklets.
   */
  private synchronized VortexFutureDelegate fetchDelegate(final List<Integer> taskletIds) {
    VortexFutureDelegate delegate = null;
    for (final int taskletId : taskletIds) {
      final VortexFutureDelegate currDelegate = taskletFutureMap.remove(taskletId);
      if (currDelegate == null) {
        // TODO[JIRA REEF-500]: Consider duplicate tasklets.
        throw new RuntimeException("Tasklet should only be removed once.");
      }

      if (delegate == null) {
        delegate = currDelegate;
      } else {
        assert delegate == currDelegate;
      }
    }

    return delegate;
  }

  @Override
  public <T> MasterCacheKey<T> cache(final String keyId, final T data, final Codec<T> codec)
      throws VortexCacheException {
    final MasterCacheKey<T> key = new MasterCacheKey<>(keyId, codec);

    if (cacheMap.getIfPresent(keyId) != null) {
      throw new VortexCacheException(keyId + "is already used as a key id for other entity.");
    }

    // TODO[REEF-1113]: Handle serialization failure separately in Vortex
    cacheMap.put(keyId, VortexAvroUtils.toBytes(new CachedDataResponse(keyId, codec.encode(data))));
    // Store the serialized bytes to reduce the serialization cost.
    return key;
  }

  @Override
  public <T> HdfsCacheKey<T>[] cache(final String path, final int numSplit, final VortexParser<?, T> parser) {
    try {
      // TODO Other type of input formats could be used?
      final ExternalConstructor<JobConf> jobConfConstructor =
          new JobConfExternalConstructor(TextInputFormat.class.getName(), path);
      final JobConf jobConf = jobConfConstructor.newInstance();
      final InputFormat inputFormat = jobConf.getInputFormat();
      final InputSplit[] splits = inputFormat.getSplits(jobConf, numSplit);

      final HdfsCacheKey[] keys = new HdfsCacheKey[numSplit];
      for (int i = 0; i < numSplit; i++) {
        final String serializedSplit = WritableSerializer.serialize(splits[i]);
        keys[i] = new HdfsCacheKey<>(path, i, serializedSplit, parser);
      }
      return keys;
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void invalidate(final CacheKey key) {
    cacheMap.invalidate(key.getId());
  }

  @Override
  public void dataRequested(final String workerId, final String keyId) throws VortexCacheException {
    final byte[] serializedData = cacheMap.getIfPresent(keyId);
    if (serializedData == null) {
      throw new VortexCacheException("The entity does not exists for the key with id: " + keyId);
    }

//    runningWorkers.sendCacheData(workerId, keyId, serializedData);
    runningWorkers.sendCacheData(workerId, serializedData);
  }
}
