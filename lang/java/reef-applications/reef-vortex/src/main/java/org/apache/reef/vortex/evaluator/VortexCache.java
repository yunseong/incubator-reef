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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.data.loading.impl.InMemoryInputFormatDataSet;
import org.apache.reef.io.data.loading.impl.InputSplitExternalConstructor;
import org.apache.reef.io.data.loading.impl.JobConfExternalConstructor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.Tang;
import org.apache.reef.vortex.common.CacheKey;
import org.apache.reef.vortex.common.HdfsCacheKey;
import org.apache.reef.vortex.common.MasterCacheKey;
import org.apache.reef.vortex.common.exceptions.VortexCacheException;

import javax.inject.Inject;
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
  private final Cache<CacheKey, Object> cache = CacheBuilder.newBuilder().softValues().build();
  private final ConcurrentHashMap<String, MasterCallback> waitingCallbacks = new ConcurrentHashMap<>();

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
  public static <T> T getData(final CacheKey<T> key) throws VortexCacheException {
    return cacheRef.load(key);
  }

  private <T> T load(final CacheKey<T> key) throws VortexCacheException {
    try {
      final Callable<T> callback;
      switch (key.getType()) {
      case MASTER:
        final MasterCacheKey<T> masterCacheKey = (MasterCacheKey<T>)key;
        callback = new MasterCallback<>(masterCacheKey);
        break;
      case HDFS:
        final HdfsCacheKey<T> hdfsCacheKey = (HdfsCacheKey<T>)key;
        callback = new HdfsCallback<>(hdfsCacheKey);
        break;

      default:
        throw new RuntimeException("Undefined type" + key.getType());
      }
      return (T) cache.get(key, callback);
    } catch (final ExecutionException e) {
      throw new VortexCacheException("Failed to fetch the data", e);
    }
  }

  /**
   * Called by VortexWorker to wakes the thread that waits for the data.
   * @param keyId Key identifier of the cache key associated with the data.
   * @param serializedData Data in a serialized form.
   */
  <T> void notifyOnArrival(final String keyId, final byte[] serializedData) {
    if (!waitingCallbacks.containsKey(keyId)) {
      throw new RuntimeException("Not requested key id: " + keyId
          + "/ number of waiting entries: " + waitingCallbacks.size());
    }

    final MasterCallback callback = waitingCallbacks.remove(keyId);
    synchronized (callback) {
      callback.onDataArrived(serializedData);
      callback.notify();
    }
  }

  final class MasterCallback<T> implements Callable<T> {
    private boolean dataArrived = false;
    private byte[] serializedData;
    private final MasterCacheKey<T> cacheKey;

    MasterCallback(final MasterCacheKey<T> cacheKey) {
      this.cacheKey = cacheKey;
    }

    void onDataArrived(final byte[] arrivedBytes) {
      synchronized (this) {
        this.serializedData = arrivedBytes;
        this.dataArrived = true;
        this.notify();
      }
    }

    @Override
    public T call() throws Exception {
      waitingCallbacks.put(cacheKey.getId(), this);
      worker.get().sendDataRequest(cacheKey);

      synchronized (this) {
        while (!dataArrived) {
          this.wait();
        }
        return cacheKey.getCodec().decode(serializedData);
      }
    }
  }

  class HdfsCallback<T> implements Callable<T> {
    private final HdfsCacheKey<T> hdfsBackedCacheKey;

    HdfsCallback(final HdfsCacheKey<T> cacheKey) {
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
}
