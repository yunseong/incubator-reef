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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import net.jcip.annotations.ThreadSafe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.htrace.*;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.data.loading.impl.JobConfExternalConstructor;
import org.apache.reef.io.data.loading.impl.WritableSerializer;
import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.util.Optional;
import org.apache.reef.vortex.api.VortexFunction;
import org.apache.reef.vortex.api.VortexFuture;
import org.apache.reef.vortex.common.MasterCacheKey;
import org.apache.reef.vortex.common.CacheSentRequest;
import org.apache.reef.vortex.common.HDFSBackedCacheKey;
import org.apache.reef.vortex.common.VortexRequest;
import org.apache.reef.vortex.common.exceptions.VortexCacheException;
import org.apache.reef.vortex.examples.lr.input.LRInputCached;
import org.apache.reef.vortex.examples.lr.input.LRInputHalfCached;
import org.apache.reef.vortex.trace.HTrace;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
  private static final String JOB_SPAN = "JobSpan";
  private final Span jobSpan;

  private final AtomicInteger taskletIdCounter = new AtomicInteger();
  private final RunningWorkers runningWorkers;
  private final PendingTasklets pendingTasklets;
  // TODO This should be replaced by Guava
  private final ConcurrentMap<String, byte[]> cacheMap = new ConcurrentHashMap<>();

  /**
   * @param runningWorkers for managing all running workers.
   */
  @Inject
  DefaultVortexMaster(final RunningWorkers runningWorkers,
                      final PendingTasklets pendingTasklets,
                      final HTrace hTrace) {
    hTrace.initialize();
    this.runningWorkers = runningWorkers;
    this.pendingTasklets = pendingTasklets;
    jobSpan = Trace.startSpan(JOB_SPAN, Sampler.ALWAYS).detach();
  }

  /**
   * Add a new tasklet to pendingTasklets.
   */
  @Override
  public <TInput, TOutput extends Serializable> VortexFuture<TOutput>
      enqueueTasklet(final VortexFunction<TInput, TOutput> function, final TInput input) {
    // TODO[REEF-500]: Simple duplicate Vortex Tasklet launch.
    final VortexFuture<TOutput> vortexFuture = new VortexFuture<>();
    final Tasklet tasklet = new Tasklet<>(
        taskletIdCounter.getAndIncrement(),
        function,
        input,
        vortexFuture,
        TraceInfo.fromSpan(jobSpan));
    this.pendingTasklets.addLast(tasklet);
    return vortexFuture;
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
      for (final Tasklet tasklet : preemptedTasklets.get()) {
        Logger.getLogger(DefaultVortexMaster.class.getName()).log(Level.INFO, "restore {0}", tasklet.getId());
        pendingTasklets.addFirst(tasklet);
      }
    }
  }

  /**
   * Notify task completion to runningWorkers.
   */
  @Override
  public void taskletCompleted(final String workerId,
                               final int taskletId,
                               final Serializable result) {
    runningWorkers.completeTasklet(workerId, taskletId, result);
  }

  /**
   * Notify task failure to runningWorkers.
   */
  @Override
  public void taskletErrored(final String workerId, final int taskletId, final Exception exception) {
    runningWorkers.errorTasklet(workerId, taskletId, exception);
  }

  @Override
  public <T extends Serializable> MasterCacheKey<T> cache(final String keyName, @Nonnull final T data)
      throws VortexCacheException {
    if (cacheMap.containsKey(keyName)) {
      throw new VortexCacheException("The keyName " + keyName + "is already used.");
    }

    final Kryo kryo = new Kryo();
    kryo.register(LRInputCached.class);
    kryo.register(LRInputHalfCached.class);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final Output output = new Output(byteArrayOutputStream);

    final MasterCacheKey key = new MasterCacheKey(keyName);
    final CacheSentRequest cacheSentRequest = new CacheSentRequest(key, data);

    kryo.writeObject(output, new VortexRequest(cacheSentRequest));
    output.close();
    final byte[] requestBytes = byteArrayOutputStream.toByteArray();

    cacheMap.put(keyName, requestBytes);
    return key;
  }

  @Override
  public HDFSBackedCacheKey[] cache(final String path, final int numSplit) {
    try {
      // TODO Other type of input formats could be used?
      final ExternalConstructor<JobConf> jobConfConstructor =
          new JobConfExternalConstructor(TextInputFormat.class.getName(), path);
      final JobConf jobConf = jobConfConstructor.newInstance();
      final InputFormat inputFormat = jobConf.getInputFormat();
      final InputSplit[] splits = inputFormat.getSplits(jobConf, numSplit);
      final String serializedJobConf =  WritableSerializer.serialize(jobConf);

      final HDFSBackedCacheKey[] keys = new HDFSBackedCacheKey[numSplit];
      for (int i = 0; i < numSplit; i++) {
        final String serializedSplit = WritableSerializer.serialize(splits[i]);
        keys[i] = new HDFSBackedCacheKey(path, i, serializedJobConf, serializedSplit);
      }
      return keys;
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void dataRequested(final String workerId, final MasterCacheKey cacheKey, final Span parentSpan)
      throws VortexCacheException {
    synchronized (cacheMap) {
      final String keyName = cacheKey.getName();
      if (!cacheMap.containsKey(keyName)) {
        throw new VortexCacheException("The entity does not exist for the key : " + cacheKey);
      }
      final byte[] serializedData = cacheMap.get(keyName);
      Logger.getLogger(DefaultVortexMaster.class.getName())
          .log(Level.INFO, "*V*fetch\t{0}\tkey\t{1}\tworker\t{2}",
              new Object[]{serializedData.length, keyName, workerId});
      runningWorkers.sendCacheData(workerId, serializedData, TraceInfo.fromSpan(parentSpan));
    }
  }

  /**
   * Terminate the job.
   */
  @Override
  public synchronized void terminate() {
    Trace.continueSpan(jobSpan).close();
    try {
      Thread.sleep(1000);
    } catch (Exception e) {
      throw new RuntimeException("sleep interrupted");
    }

    runningWorkers.terminate();
  }
}
