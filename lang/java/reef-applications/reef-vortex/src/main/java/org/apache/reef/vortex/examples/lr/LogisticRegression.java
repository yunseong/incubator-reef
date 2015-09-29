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

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.vortex.driver.VortexLauncher;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * User's main function.
 */
public final class LogisticRegression {
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
          .registerShortNameOfClass(ZipkinCollector.class)
          .registerShortNameOfClass(NumWorkers.class)
          .registerShortNameOfClass(WorkerCores.class)
          .registerShortNameOfClass(WorkerCapacity.class)
          .registerShortNameOfClass(WorkerMemMb.class)
          .processCommandLine(args);

      final Injector injector = tang.newInjector(cb.build());

      final boolean isLocal = injector.getNamedInstance(Local.class);
      final int numOfWorkers = injector.getNamedInstance(NumWorkers.class);
      final int workerCores = injector.getNamedInstance(WorkerCores.class);
      final int workerCapacity = injector.getNamedInstance(WorkerCapacity.class);
      final int workerMemory = injector.getNamedInstance(WorkerMemMb.class);

      Logger.getLogger(LogisticRegression.class.getName())
          .log(Level.INFO, "Config: [worker] {0} / [mem] {1} / [cores] {2} / [capacity] {3}",
          new Object[]{numOfWorkers, workerMemory, workerCores, workerCapacity});

      if (isLocal) {
        VortexLauncher.launchLocal("Vortex_LR", LogisticRegressionStart.class,
            numOfWorkers, workerMemory, workerCores, workerCapacity);
      } else {
        VortexLauncher.launchYarn("Vortex_LR", LogisticRegressionStart.class,
            numOfWorkers, workerMemory, workerCores, workerCapacity);
      }

    } catch (final IOException | InjectionException e) {
      throw new RuntimeException(e);
    }
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
      short_name = "worker_capacity", default_value = "16")
  public static final class WorkerCapacity implements Name<Integer> {
  }

  @NamedParameter(short_name = "zipkin_collector")
  public static final class ZipkinCollector implements Name<String> {
  }
}
