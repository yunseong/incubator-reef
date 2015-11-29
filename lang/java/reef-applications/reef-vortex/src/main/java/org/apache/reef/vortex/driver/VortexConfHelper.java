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

import org.apache.reef.annotations.Unstable;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.util.Optional;
import org.apache.reef.vortex.api.VortexStart;

import java.io.IOException;

/**
 * Helper class for building a configuration for Vortex.
 */
@Unstable
public final class VortexConfHelper {
  private VortexConfHelper() {
  }

  private static final int DEFAULT_NUM_OF_VORTEX_START_THREAD = 1;

  /**
   * @return Configuration for Vortex job.
   */
  public static Configuration getVortexConf(final String jobName,
                                            final Class<? extends VortexStart> vortexStart,
                                            final int numOfWorkers,
                                            final int workerMemory,
                                            final int workerCores,
                                            final int workerCapacity,
                                            final String[] args,
                                            final Optional<CommandLine> cmdLine) throws IOException {
    final Configuration vortexDriverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(VortexDriver.class))
        .set(DriverConfiguration.ON_DRIVER_STARTED, VortexDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, VortexDriver.AllocatedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING, VortexDriver.RunningTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_MESSAGE, VortexDriver.TaskMessageHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_FAILED, VortexDriver.FailedEvaluatorHandler.class)
        .set(DriverConfiguration.DRIVER_IDENTIFIER, jobName)
        .build();

    final Configuration vortexMasterConf = VortexMasterConf.CONF
        .set(VortexMasterConf.WORKER_NUM, numOfWorkers)
        .set(VortexMasterConf.WORKER_MEM, workerMemory)
        .set(VortexMasterConf.WORKER_CORES, workerCores)
        .set(VortexMasterConf.WORKER_CAPACITY, workerCapacity)
        .set(VortexMasterConf.VORTEX_START, vortexStart)
        .set(VortexMasterConf.NUM_OF_VORTEX_START_THREAD, DEFAULT_NUM_OF_VORTEX_START_THREAD) // fixed to 1 for now
        .build();

    final Configuration commandLineConf;
    if (cmdLine.isPresent()) {
      final ConfigurationBuilder configurationBuilder = cmdLine.get().getBuilder();
      // Named parameters can be registered here.
      cmdLine.get().processCommandLine(args);
      commandLineConf = configurationBuilder.build();
    } else {
      commandLineConf = Tang.Factory.getTang().newConfigurationBuilder().build();
    }

    return Configurations.merge(vortexDriverConf, vortexMasterConf, commandLineConf);
  }
}
