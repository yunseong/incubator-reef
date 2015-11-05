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
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.vortex.common.CacheKey;
import org.apache.reef.vortex.driver.VortexWorkerConf;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * External constructor for creating GuavaCache.
 */
public final class CacheConstructor implements ExternalConstructor<Cache> {
  private static final Logger LOG = Logger.getLogger(CacheConstructor.class.getName());
  private final int numThreads;

  @Inject
  private CacheConstructor(@Parameter(VortexWorkerConf.NumOfSlackThreads.class) final int slackThreads) {
    numThreads = slackThreads;
  }

  @Override
  public Cache newInstance() {
    return CacheBuilder.newBuilder().removalListener(removalListener).concurrencyLevel(numThreads).build();
  }

  private final RemovalListener<CacheKey, Serializable> removalListener =
      new RemovalListener<CacheKey, Serializable>() {
        @Override
        public void onRemoval(final RemovalNotification<CacheKey, Serializable> removalNotification) {
          LOG.log(Level.INFO, "{0} is removed", removalNotification.getKey());
        }
      };
}
