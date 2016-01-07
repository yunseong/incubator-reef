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

package org.apache.reef.vortex.examples.als;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.vortex.driver.VortexLauncher;
import org.apache.reef.vortex.failure.FailureParameters;
import org.apache.reef.vortex.trace.HTraceParameters;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class AlternatingLeastSquares {
  private AlternatingLeastSquares() {
  }

  public static void main(final String[] args) {

    final Tang tang = Tang.Factory.getTang();

    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();

    try {
      final CommandLine cl = new CommandLine(cb);
      HTraceParameters.registerShortNames(cl);
      FailureParameters.registerShortNames(cl);
      cl.registerShortNameOfClass(Local.class)
          .registerShortNameOfClass(NumWorkers.class)
          .registerShortNameOfClass(WorkerCores.class)
          .registerShortNameOfClass(WorkerCapacity.class)
          .registerShortNameOfClass(WorkerMemMb.class)
          .registerShortNameOfClass(DivideFactor.class)
          .registerShortNameOfClass(NumIter.class)
          .registerShortNameOfClass(UserDataMatrixPath.class)
          .registerShortNameOfClass(ItemDataMatrixPath.class)
          .registerShortNameOfClass(NumUsers.class)
          .registerShortNameOfClass(NumItems.class)
          .registerShortNameOfClass(NumFeatures.class)
          .registerShortNameOfClass(SaveModel.class)
          .registerShortNameOfClass(DriverMemory.class)
          .registerShortNameOfClass(PrintMSE.class)
          .processCommandLine(args);

      final Injector injector = tang.newInjector(cb.build());

      final boolean isLocal = injector.getNamedInstance(Local.class);
      final int numOfWorkers = injector.getNamedInstance(NumWorkers.class);
      final int workerCores = injector.getNamedInstance(WorkerCores.class);
      final int workerCapacity = injector.getNamedInstance(WorkerCapacity.class);
      final int workerMemory = injector.getNamedInstance(WorkerMemMb.class);
      final int driverMemory = injector.getNamedInstance(DriverMemory.class);

      cb.bindNamedParameter(org.apache.reef.driver.parameters.DriverMemory.class, driverMemory + "");

      Logger.getLogger(AlternatingLeastSquares.class.getName())
          .log(Level.INFO, "Config: [worker] {0} / [mem] {1} / [cores] {2} / [capacity] {3} / [driver mem] {4}",
              new Object[]{numOfWorkers, workerMemory, workerCores, workerCapacity, driverMemory});

      final Class startClass = ALSVortexStart.class;
      if (isLocal) {
        VortexLauncher.launchLocal("Vortex_ALS", startClass,
            numOfWorkers, workerMemory, workerCores, workerCapacity, cb.build());
      } else {
        VortexLauncher.launchYarn("Vortex_ALS", startClass,
            numOfWorkers, workerMemory, workerCores, workerCapacity, cb.build());
      }

    } catch (final IOException | InjectionException e) {
      throw new RuntimeException(e);
    }
  }

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

  @NamedParameter(short_name = "divide_factor", default_value = "8")
  public static final class DivideFactor implements Name<Integer> {
  }

  @NamedParameter(short_name = "num_iter", default_value = "10")
  public static final class NumIter implements Name<Integer> {
  }

  @NamedParameter(short_name = "lambda", default_value = "0.05")
  public static final class Lambda implements Name<Double> {
  }

  @NamedParameter(short_name = "um_path")
  public static final class UserDataMatrixPath implements Name<String> {
  }

  @NamedParameter(short_name = "im_path")
  public static final class ItemDataMatrixPath implements Name<String> {
  }

  @NamedParameter(short_name = "num_items")
  public static final class NumItems implements Name<Integer> {
  }

  @NamedParameter(short_name = "num_users")
  public static final class NumUsers implements Name<Integer> {
  }

  @NamedParameter(short_name = "num_features", default_value = "10")
  public static final class NumFeatures implements Name<Integer> {
  }

  @NamedParameter(short_name = "save_model", default_value = "false")
  public static final class SaveModel implements Name<Boolean> {
  }

  @NamedParameter(short_name = "print_mse", default_value = "false")
  public static final class PrintMSE implements Name<Boolean> {
  }

  @NamedParameter(short_name = "driver_memory", default_value = "7168")
  public static final class DriverMemory implements Name<Integer> {
  }
}
