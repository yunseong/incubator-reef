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
package org.apache.reef.vortex.trace;

import org.apache.htrace.Sampler;
import org.apache.htrace.SpanReceiver;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

import javax.inject.Inject;

/**
 * Instantiated by Tang.
 */
public final class HTrace {

  @Inject
  private HTrace(final SpanReceiver spanReceiver) {
    Trace.addReceiver(spanReceiver);
    initialTrace();
  }

  /**
   * We've noticed a ~200ms delay when calling startSpan for the first time.
   * Calling initialTrace here moves this delay from trace-time to Tang construction time.
   * The reason for the delay is conjectured to be the lazy initialization at {@link org.apache.htrace.Tracer}
   */
  private void initialTrace() {
    final TraceScope traceScope = Trace.startSpan("initialTrace", Sampler.ALWAYS);
    traceScope.close();
  }

  /**
   * Initialize HTrace.
   */
  public void initialize() {
    // Left empty, because the constructor does the initialization.
    // The method is here as a reminder that an instance of this class must be
    // injected before using Trace.[static method] calls.

  }
}
