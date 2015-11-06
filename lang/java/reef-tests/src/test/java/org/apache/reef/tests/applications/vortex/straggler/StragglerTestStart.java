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
package org.apache.reef.tests.applications.vortex.straggler;

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
 * Test correctness of scheduling stragglers. Tasklets finished within either 1s or 10s,
 * and this is determined randomly. Since the StragglerHandlingScheduler reschedules the
 * Straggler, the Tasklets' running time should be all 1s if they are complete correctly.
 * TODO But this is done up to 3 times. Is there a way to solve this deterministically?
 */
public final class StragglerTestStart implements VortexStart {
  private static final Logger LOG = Logger.getLogger(StragglerTestStart.class.getName());

  @Inject
  private StragglerTestStart() {
  }

  @Override
  public void start(final VortexThreadPool vortexThreadPool) {
    final int numTasklets = 100;

    final long startTime = System.currentTimeMillis();

    final List<VortexFuture<Long>> futures = new ArrayList<>(numTasklets);

    for (int i = 0; i < numTasklets; i++) {
      final boolean isStraggler = i % 2 == 0;
      futures.add(vortexThreadPool.submit(new StragglerFunction(), isStraggler));
    }

    try {
      for (int i = 0; i < numTasklets; i++) {
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
