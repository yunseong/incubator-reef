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
package org.apache.reef.vortex.examples.lr;

import org.apache.reef.driver.parameters.DriverMemory;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.vortex.api.VortexStart;
import org.apache.reef.vortex.driver.VortexJobConf;
import org.apache.reef.vortex.driver.VortexLauncher;
import org.apache.reef.vortex.driver.VortexMasterConf;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * User's main function.
 */
public final class LogisticRegression {
  private static final Logger LOG = Logger.getLogger(LogisticRegression.class.getName());

  private LogisticRegression() {
  }

  /**
   * Launch the vortex job, passing appropriate arguments.
   */
  public static void main(final String[] args) {
    final Tang tang = Tang.Factory.getTang();
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();

    try {
      new CommandLine(cb)
          .registerShortNameOfClass(Local.class)
          .registerShortNameOfClass(NumWorkers.class)
          .registerShortNameOfClass(WorkerCores.class)
          .registerShortNameOfClass(WorkerCapacity.class)
          .registerShortNameOfClass(WorkerMemMb.class)
          .registerShortNameOfClass(DivideFactor.class)
          .registerShortNameOfClass(NumIter.class)
          .registerShortNameOfClass(Path.class)
          .registerShortNameOfClass(ModelDim.class)
          .registerShortNameOfClass(DriverMem.class)
          .processCommandLine(args);

      final Injector injector = tang.newInjector(cb.build());

      final boolean isLocal = injector.getNamedInstance(Local.class);

      final int numOfWorkers = injector.getNamedInstance(NumWorkers.class);
      final int workerCores = injector.getNamedInstance(WorkerCores.class);
      final int workerCapacity = injector.getNamedInstance(WorkerCapacity.class);
      final int workerMemory = injector.getNamedInstance(WorkerMemMb.class);

      final int divideFactor = injector.getNamedInstance(DivideFactor.class);
      final String path = injector.getNamedInstance(Path.class);
      final int modelDim = injector.getNamedInstance(ModelDim.class);
      final int driverMem = injector.getNamedInstance(DriverMem.class);

      LOG.log(Level.FINE, "Starting LR. Divide factor {0} / Path {1} / Model dim {2} / Local {3}",
          new Object[] {divideFactor, path, modelDim, isLocal});
      LOG.log(Level.FINE, "Number of workers {0} / Worker cores {1} / Worker capacity {2} / Worker memory {3}",
          new Object[] {numOfWorkers, workerCores, workerCapacity, workerMemory, numOfWorkers});

      final Configuration vortexMasterConf = getVortexMasterConfig(LogisticRegressionStart.class,
          workerCores, workerCapacity, workerMemory, numOfWorkers);

      cb.bindNamedParameter(DriverMemory.class, String.valueOf(driverMem));
      final VortexJobConf vortexConf = VortexJobConf.newBuilder()
          .setJobName("VortexLR")
          .setVortexMasterConf(vortexMasterConf)
          .setUserConf(cb.build())
          .build();

      final Configuration runtimeConf = getRuntimeConf(isLocal);

      VortexLauncher.launch(runtimeConf, vortexConf.getConfiguration());

    } catch (final IOException | InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  private static Configuration getVortexMasterConfig(final Class<? extends VortexStart> startClass,
                                                     final int workerCores,
                                                     final int workerCapacity,
                                                     final int workerMemory,
                                                     final int numOfWorkers) {
    return VortexMasterConf.CONF
        .set(VortexMasterConf.VORTEX_START, startClass)
        .set(VortexMasterConf.WORKER_CORES, workerCores)
        .set(VortexMasterConf.WORKER_CAPACITY, workerCapacity)
        .set(VortexMasterConf.WORKER_MEM, workerMemory)
        .set(VortexMasterConf.WORKER_NUM, numOfWorkers)
        .build();
  }

  private static Configuration getRuntimeConf(final boolean isLocal) {
    final Configuration runtimeConf;
    if (isLocal) {
      runtimeConf = LocalRuntimeConfiguration.CONF.build();
    } else {
      runtimeConf = YarnClientConfiguration.CONF.build();
    }
    return runtimeConf;
  }

  /**
   * Command line parameter = true to run locally, or false to run on YARN.
   */
  @NamedParameter(doc = "Whether or not to run on the local runtime",
      short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {
  }

  @NamedParameter(doc = "Number of workers",
      short_name = "num_workers", default_value = "1")
  public static final class NumWorkers implements Name<Integer> {
  }

  @NamedParameter(doc = "Worker memory",
      short_name = "worker_mem_mb", default_value = "8192")
  public static final class WorkerMemMb implements Name<Integer> {
  }

  @NamedParameter(doc = "Number of cores each worker has",
      short_name = "worker_cores", default_value = "4")
  public static final class WorkerCores implements Name<Integer> {
  }

  @NamedParameter(doc = "Worker capacity",
      short_name = "worker_capacity", default_value = "4")
  public static final class WorkerCapacity implements Name<Integer> {
  }

  @NamedParameter(short_name = "divide_factor", default_value = "8")
  public static final class DivideFactor implements Name<Integer> {
  }

  @NamedParameter(short_name = "num_iter", default_value = "10")
  public static final class NumIter implements Name<Integer> {
  }

  @NamedParameter(short_name = "path")
  public static final class Path implements Name<String> {
  }

  @NamedParameter(short_name = "model_dim", default_value = "3231961")
  public static final class ModelDim implements Name<Integer> {
  }

  @NamedParameter(short_name = "driver_mem", default_value = "7340032")
  public static final class DriverMem implements Name<Integer> {
  }
}
