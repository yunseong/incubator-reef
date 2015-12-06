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
package org.apache.reef.vortex.driver;

import org.apache.htrace.SpanReceiver;
import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;
import org.apache.reef.vortex.trace.ReceiverConstructor;
import org.apache.reef.vortex.trace.parameters.ProcessName;
import org.apache.reef.vortex.trace.parameters.ReceiverHost;
import org.apache.reef.vortex.trace.parameters.ReceiverPort;
import org.apache.reef.vortex.trace.parameters.ReceiverType;

/**
 *  Vortex Worker configuration.
 */
@Unstable
@DriverSide
public final class VortexWorkerConf extends ConfigurationModuleBuilder {
  /**
   * Worker Threads.
   */
  @NamedParameter(doc = "Number of Worker Threads")
  public final class NumOfThreads implements Name<Integer> {
  }

  /**
   * Slack Threads for handling messages when all threads are blocked due to the cache miss.
   */
  @NamedParameter(doc = "Number of slack Threads", default_value = "2")
  public final class NumOfSlackThreads implements Name<Integer> {
  }

  /**
   * Worker Threads.
   */
  public static final RequiredParameter<Integer> NUM_OF_THREADS = new RequiredParameter<>();

  /**
   * Slack Threads.
   */
  public static final OptionalParameter<Integer> NUM_OF_SLACK_THREADS = new OptionalParameter<>();

  public static final OptionalParameter<String> RECEIVER_TYPE = new OptionalParameter<>();
  public static final OptionalParameter<String> RECEIVER_HOST = new OptionalParameter<>();
  public static final OptionalParameter<Integer> RECEIVER_PORT = new OptionalParameter<>();

  /**
   * Vortex Worker configuration.
   */
  public static final ConfigurationModule CONF = new VortexWorkerConf()
      .bindNamedParameter(NumOfThreads.class, NUM_OF_THREADS)
      .bindNamedParameter(NumOfSlackThreads.class, NUM_OF_SLACK_THREADS)
      .bindNamedParameter(ProcessName.class, "vWorker")
      .bindNamedParameter(ReceiverType.class, RECEIVER_TYPE)
      .bindNamedParameter(ReceiverHost.class, RECEIVER_HOST)
      .bindNamedParameter(ReceiverPort.class, RECEIVER_PORT)
      .bindConstructor(SpanReceiver.class, ReceiverConstructor.class)
      .build();
}
