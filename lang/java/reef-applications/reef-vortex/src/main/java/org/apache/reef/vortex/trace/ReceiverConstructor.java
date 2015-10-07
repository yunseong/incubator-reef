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

import org.apache.htrace.HTraceConfiguration;
import org.apache.htrace.SpanReceiver;
import org.apache.htrace.impl.StandardOutSpanReceiver;
import org.apache.htrace.impl.ZipkinSpanReceiver;
import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.vortex.trace.parameters.ProcessName;
import org.apache.reef.vortex.trace.parameters.ReceiverHost;
import org.apache.reef.vortex.trace.parameters.ReceiverPort;
import org.apache.reef.vortex.trace.parameters.ReceiverType;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

/**
 * External constructor for Span receiver.
 */
public final class ReceiverConstructor implements ExternalConstructor<SpanReceiver> {

  public static final String ZIPKIN = "ZIPKIN";
  public static final String STDOUT = "STDOUT";

  private final SpanReceiver receiver;

  @Inject
  private ReceiverConstructor(@Parameter(ReceiverType.class) final String receiverType,
                              @Parameter(ReceiverHost.class) final String receiverHost,
                              @Parameter(ReceiverPort.class) final int receiverPort,
                              @Parameter(ProcessName.class) final String processName) {
    if (STDOUT.equals(receiverType)) {
      this.receiver = new StandardOutSpanReceiver(HTraceConfiguration.EMPTY);
    } else if (ZIPKIN.equals(receiverType)) {
      this.receiver = getZipkinReceiver(receiverHost, receiverPort, processName);
    } else {
      throw new RuntimeException("Unknown receiverType " + receiverType);
    }
  }

  private static ZipkinSpanReceiver getZipkinReceiver(final String receiverHost, final int receiverPort,
                                                      final String processName) {
    final Map<String, String> confMap = new HashMap<>(2);
    confMap.put("process.id", processName + "_" + System.currentTimeMillis());
    confMap.put("zipkin.collector-hostname", receiverHost);
    confMap.put("zipkin.collector-port", Integer.toString(receiverPort));

    final ZipkinSpanReceiver receiver = new ZipkinSpanReceiver(HTraceConfiguration.fromMap(confMap));
    return receiver;
  }

  @Override
  public SpanReceiver newInstance() {
    return receiver;
  }
}
