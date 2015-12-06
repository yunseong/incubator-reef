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
package org.apache.reef.vortex.evaluator;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.htrace.Span;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceInfo;
import org.apache.htrace.TraceScope;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.data.loading.impl.InMemoryInputFormatDataSet;
import org.apache.reef.io.data.loading.impl.InputSplitExternalConstructor;
import org.apache.reef.io.data.loading.impl.JobConfExternalConstructor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.Tang;
import org.apache.reef.vortex.common.CacheKey;
import org.apache.reef.vortex.common.MasterCacheKey;
import org.apache.reef.vortex.common.HDFSBackedCacheKey;
import org.apache.reef.vortex.common.exceptions.VortexCacheException;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

/**
 * Caches the data. Users can access the data by calling {@link #getData(CacheKey)} in the user code.
 * If the data does not exist yet, then the cache fetches it from the Driver and returns the loaded data.
 */
public final class VortexCache {
  private static final Logger LOG = Logger.getLogger(VortexCache.class.getName());

  private static VortexCache cacheRef;
  private final Cache<CacheKey, Serializable> cache = CacheBuilder.newBuilder().build();
  private final ConcurrentHashMap<MasterCacheKey, CustomCallable> waiters = new ConcurrentHashMap<>();

  private final InjectionFuture<VortexWorker> worker;

  @Inject
  private VortexCache(final InjectionFuture<VortexWorker> worker) {
    this.worker = worker;
    this.cacheRef = VortexCache.this;
  }

  /**
   * @param key Key of the Data.
   * @param <T> Type of the Data.
   * @return The data from the cache. If the data does not exist in cache, the thread is blocked until the data arrives.
   * @throws VortexCacheException If it fails to fetch the data.
   */
  public static <T extends Serializable> T getData(final CacheKey<T> key) throws VortexCacheException {
    final Span currentSpan = Trace.currentSpan();
    try (final TraceScope getDataScope = Trace.startSpan("cache_get_"+key.getId(), currentSpan)) {
      return cacheRef.load(key, getDataScope);
    } finally {
      Trace.continueSpan(currentSpan);
    }
  }

  private <T extends Serializable> T load(final CacheKey<T> key, final TraceScope parentScope)
      throws VortexCacheException {
    try {
      final Callable<T> callable;
      switch (key.getType()) {
      case HDFS:
        final HDFSBackedCacheKey<T> hdfsCacheKey = (HDFSBackedCacheKey<T>)key;
        callable = new HdfsCallable<T>(hdfsCacheKey);
        break;
      case MASTER:
        final MasterCacheKey<T> masterCacheKey = (MasterCacheKey<T>)key;
        callable = new CustomCallable<T>(masterCacheKey, parentScope);
        break;
      default:
        throw new RuntimeException("Undefined type" + key.getType());
      }
      return (T) cache.get(key, callable);
    } catch (final ExecutionException e) {
      throw new VortexCacheException("Failed to fetch the data", e);
    }
  }

  class HdfsCallable<T extends Serializable> implements Callable<T> {
    private final HDFSBackedCacheKey<T> hdfsBackedCacheKey;

    HdfsCallable(final HDFSBackedCacheKey<T> cacheKey) {
      this.hdfsBackedCacheKey= cacheKey;
    }

    @Override
    public T call() throws Exception {
      final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindImplementation(DataSet.class, InMemoryInputFormatDataSet.class)
          .bindConstructor(InputSplit.class, InputSplitExternalConstructor.class)
          .bindConstructor(JobConf.class, JobConfExternalConstructor.class)
          .bindNamedParameter(InputSplitExternalConstructor.SerializedInputSplit.class,
              hdfsBackedCacheKey.getSerializedInputSplit())
          .bindNamedParameter(JobConfExternalConstructor.InputFormatClass.class, TextInputFormat.class.getName())
          .bindNamedParameter(JobConfExternalConstructor.InputPath.class, hdfsBackedCacheKey.getPath())
          .build();
      final DataSet<LongWritable, Text> dataSet =
          Tang.Factory.getTang().newInjector(conf).getInstance(DataSet.class);

      return (T) hdfsBackedCacheKey.getParser().parse(dataSet);
    }
  }



  /**
   * Called by VortexWorker to wakes the thread that waits for the data.
   * @param key Key of the data.
   * @param data Data itself.
   */
  void notifyOnArrival(final MasterCacheKey key, final Serializable data) {
    if (!waiters.containsKey(key)) {
      throw new RuntimeException("Not requested key: " + key + "waiters size : " + waiters.size());
    }

    final CustomCallable waiter = waiters.remove(key);
    synchronized (waiter) {
      waiter.onDataArrived(data);
      waiter.notify();
    }
  }

  final class CustomCallable<T extends Serializable> implements Callable<T> {
    private boolean dataArrived = false;
    private T waitingData;
    private final MasterCacheKey<T> cacheKey;
    private final Span callableSpan;

    CustomCallable(final MasterCacheKey<T> cacheKey, final TraceScope parentScope) {
      this.cacheKey = cacheKey;
      this.callableSpan = parentScope.detach();
    }

    void onDataArrived(final T data) {
      synchronized (this) {
        this.waitingData = data;
        this.dataArrived = true;
        this.notify();
      }
    }

    T getData() {
      return waitingData;
    }

    @Override
    public T call() throws Exception {
      final TraceInfo traceInfo = TraceInfo.fromSpan(callableSpan);
      try (final TraceScope scope = Trace.startSpan("send_data_request", traceInfo)) {
        waiters.put(cacheKey, this);
        worker.get().sendDataRequest(cacheKey, traceInfo);
      }

      synchronized (this) {
        while (!dataArrived) {
          this.wait();
        }
      }
      Trace.continueSpan(callableSpan).close();
      return waitingData;
    }
  }
}
