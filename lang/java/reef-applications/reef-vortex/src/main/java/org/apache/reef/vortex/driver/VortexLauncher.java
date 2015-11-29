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
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.Optional;
import org.apache.reef.vortex.api.VortexStart;

import java.io.IOException;

/**
 * Launches a Vortex Job.
 */
@Unstable
public final class VortexLauncher {
  private VortexLauncher() {
  }

  private static final int MAX_NUMBER_OF_EVALUATORS = 10;

  /**
   * Launch a Vortex job using the local runtime.
   */
  public static LauncherStatus launchLocal(final String jobName,
                                           final Class<? extends VortexStart> vortexUserCode,
                                           final int numOfWorkers,
                                           final int workerMemory,
                                           final int workerCores,
                                           final int workerCapacity,
                                           final String[] args,
                                           final Optional<CommandLine> cmdLine) throws IOException {
    final Configuration runtimeConf = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
        .build();
    final Configuration vortexConf = VortexConfHelper.getVortexConf(
        jobName,
        vortexUserCode,
        numOfWorkers,
        workerMemory,
        workerCores,
        workerCapacity,
        args,
        cmdLine);
    return launch(runtimeConf, vortexConf);
  }

  private static LauncherStatus launch(final Configuration runtimeConf, final Configuration vortexConf) {
    try {
      return DriverLauncher.getLauncher(runtimeConf).run(vortexConf);
    } catch (InjectionException e) {
      throw new RuntimeException(e);
    }
  }
}
