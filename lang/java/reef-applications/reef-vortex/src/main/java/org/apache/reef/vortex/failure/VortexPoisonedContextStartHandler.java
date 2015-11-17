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
package org.apache.reef.vortex.failure;

import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.vortex.failure.parameters.IntervalMs;
import org.apache.reef.vortex.failure.parameters.Probability;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.Alarm;

import javax.inject.Inject;
import java.util.Random;

/**
 * Inject failure in the running context.
 */
public final class VortexPoisonedContextStartHandler implements EventHandler<ContextStart> {
  private static final int INITIAL_DELAY_MS = 5000; // to prevent failure before task launch
  private final Clock clock;
  private final double probability;
  private final int intervalMs;
  private final Random random = new Random();

  @Inject
  private VortexPoisonedContextStartHandler(final Clock clock,
                                            @Parameter(Probability.class) final double probability,
                                            @Parameter(IntervalMs.class) final int intervalMs) {
    this.clock = clock;
    this.probability = probability;
    this.intervalMs = intervalMs;
  }

  @Override
  public void onNext(final ContextStart contextStart) {
    // We can make sure the failure is guranteed to occur after interval(sec).
    clock.scheduleAlarm(INITIAL_DELAY_MS, new AlarmHandler(clock, probability, intervalMs));
  }

  private final class AlarmHandler implements EventHandler<Alarm> {
    private Clock clock;
    private final double probability;
    private final int interval;

    AlarmHandler(final Clock clock, final double probability, final int interval) {
      this.clock = clock;
      this.probability = probability;
      this.interval = interval;
    }

    @Override
    public void onNext(final Alarm value) {
      if (random.nextDouble() <= probability) {
        throw new RuntimeException("Crashed at: " + System.currentTimeMillis());
      } else {
        clock.scheduleAlarm(interval, this);
      }
    }
  }
}
