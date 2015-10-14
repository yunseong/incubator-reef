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
import org.apache.reef.vortex.failure.parameters.Interval;
import org.apache.reef.vortex.failure.parameters.Probability;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.Alarm;

import javax.inject.Inject;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Inject failure in the running context.
 */
public final class VortexPoisonedContextStartHandler implements EventHandler<ContextStart> {
  private static final Logger LOG = Logger.getLogger(VortexPoisonedContextStartHandler.class.getName());
  private final Clock clock;
  private final double probability;
  private final int intervalSec;

  @Inject
  private VortexPoisonedContextStartHandler(final Clock clock,
                                            @Parameter(Probability.class) final double probability,
                                            @Parameter(Interval.class) final int intervalMs) {
    this.clock = clock;
    this.probability = probability;
    this.intervalSec = intervalMs * 1000;
  }

  @Override
  public void onNext(final ContextStart contextStart) {
    // We can make sure the failure is guranteed to occur after interval(sec).
    clock.scheduleAlarm(intervalSec, new AlarmHandler(clock, probability, intervalSec));
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
      if (new Random().nextDouble() <= probability) {
        throw new RuntimeException("Crashed at: " + System.currentTimeMillis());
      } else {
        clock.scheduleAlarm(interval, this);
      }
    }
  }
}
