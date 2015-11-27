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
package org.apache.reef.examples.prod;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.runtime.yarn.client.YarnDriverConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A REEF job that mimics Production workload.
 */
public final class ProductionREEF {
  private static final Logger LOG = Logger.getLogger(ProductionREEF.class.getName());

  public static void main(final String[] args) {
    try {
      final Configuration commandLineConf = parseCommandLine(args);
      final Configuration runtimeConfig =
          YarnClientConfiguration.CONF.set(YarnClientConfiguration.YARN_PRIORITY, 5).build();

      final Configuration driverConfig = DriverConfiguration.CONF
          .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(ProductionDriver.class))
          .set(DriverConfiguration.DRIVER_IDENTIFIER, "ProductionREEF")
          .set(DriverConfiguration.ON_DRIVER_STARTED, ProductionDriver.StartHandler.class)
          .set(DriverConfiguration.ON_DRIVER_STOP, ProductionDriver.StopHandler.class)
          .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, ProductionDriver.EvaluatorAllocatedHandler.class)
          .build();

      final Configuration yarnDriverConfig = YarnDriverConfiguration.CONF
          .set(YarnDriverConfiguration.QUEUE, "prod")
          .build();
      final Configuration submittedConfiguration = Tang.Factory.getTang()
          .newConfigurationBuilder(driverConfig, commandLineConf, yarnDriverConfig).build();
      DriverLauncher.getLauncher(runtimeConfig)
          .run(submittedConfiguration);

    } catch (final BindException | InjectionException | IOException ex) {
      LOG.log(Level.SEVERE, "Job configuration error", ex);
    }
  }

  private static Configuration parseCommandLine(final String[] args)
      throws BindException, IOException {
    final ConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(confBuilder);
    cl.registerShortNameOfClass(NumTasks.class);
    cl.registerShortNameOfClass(Delay.class);
    cl.processCommandLine(args);
    return confBuilder.build();
  }

  /**
   * Command line parameter: number of Tasks to run.
   */
  @NamedParameter(doc = "Number of tasks to run", short_name = "tasks")
  public static final class NumTasks implements Name<Integer> {
  }

  /**
   * Command line parameter: number of experiments to run.
   */
  @NamedParameter(doc = "Number of seconds to sleep in each task", short_name = "delay")
  public static final class Delay implements Name<Integer> {
  }
}
