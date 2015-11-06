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

import org.apache.reef.vortex.api.VortexFunction;

import java.util.logging.Logger;

/**
 * User function that sleeps. The amount of sleep time is determined by
 * the boolean input, Tasklets sleep longer if it is one of stragglers. This returns
 * the execution time.
 *
 * For simplicity, we assume the list of stragglers is passed via input in a comma separated string.
 */
public final class StragglerFunction implements VortexFunction<Boolean, Long> {
  private static final Logger LOG = Logger.getLogger(StragglerFunction.class.getName());
  private static final int NORMAL_SLEEP_TIME_MS = 1000;
  private static final int STRAGGLER_SLEEP_TIME_MS = 10000;

  @Override
  public Long call(final Boolean straggler) throws Exception {
    final long startTime = System.currentTimeMillis();

    final int sleepTime = straggler? STRAGGLER_SLEEP_TIME_MS : NORMAL_SLEEP_TIME_MS;
    Thread.sleep(sleepTime);

    final long endTime = System.currentTimeMillis();
    return endTime - startTime;
  }
}
