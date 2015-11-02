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
package org.apache.reef.vortex.examples.straggler;

import org.apache.reef.vortex.api.VortexFuture;
import org.apache.reef.vortex.api.VortexStart;
import org.apache.reef.vortex.api.VortexThreadPool;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Master-side user code that injects stragglers in a probability.
 */
public final class StragglerStart implements VortexStart {
  private static final Logger LOG = Logger.getLogger(StragglerStart.class.getName());

  @Inject
  private StragglerStart() {
  }

  private static final int NUM_TASKLETS = 400;

  @Override
  public void start(final VortexThreadPool vortexThreadPool) {
    final long startTime = System.currentTimeMillis();

    final List<VortexFuture<Long>> futures = new ArrayList<>(NUM_TASKLETS);

    for (int i = 0; i < NUM_TASKLETS; i++) {
      futures.add(vortexThreadPool.submit(new StragglerFunction(), "172.22.151.235,172.22.151.236"));
    }

    try {
      for (int i = 0; i < NUM_TASKLETS; i++) {
        final VortexFuture<Long> future = futures.get(i);
        final long result = future.get();
        LOG.log(Level.INFO, "Result: time {0}", result);
      }
      LOG.log(Level.INFO, "#JCT: {0}", System.currentTimeMillis() - startTime);
    } catch (final InterruptedException | ExecutionException e) {
      LOG.log(Level.WARNING, "Error occurred", e);
    }
  }
}
