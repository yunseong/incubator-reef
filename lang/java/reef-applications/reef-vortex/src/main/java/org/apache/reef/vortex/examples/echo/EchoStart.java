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
package org.apache.reef.vortex.examples.echo;

import org.apache.reef.vortex.api.VortexFuture;
import org.apache.reef.vortex.api.VortexStart;
import org.apache.reef.vortex.api.VortexThreadPool;
import org.apache.reef.vortex.common.CacheKey;
import org.apache.reef.vortex.common.exceptions.VortexCacheException;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

/**
 * Echo user code.
 */
public final class EchoStart implements VortexStart {
  private static final int NUM_TASKLETS = 6;

  @Inject
  private EchoStart() {
  }

  @Override
  public void start(final VortexThreadPool vortexThreadPool) {
    final List<VortexFuture<String>> futures = new ArrayList<>();

    try {
      final CacheKey<String> quoteKey = vortexThreadPool.cache("quote", "echo");
      final EchoInput input = new EchoInput(quoteKey);
      final EchoFunction echoFunction = new EchoFunction();
      for (int i = 0; i < NUM_TASKLETS; i++) {
        futures.add(vortexThreadPool.submit(echoFunction, input));
      }
    } catch (VortexCacheException e) {
      e.printStackTrace();
    }

    final Vector<String> outputVector = new Vector<>();
    for (final VortexFuture<String> future : futures) {
      try {
        outputVector.add(future.get());
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    System.out.println("RESULT:");
    System.out.println(outputVector);
  }
}
