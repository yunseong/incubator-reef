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
package org.apache.reef.vortex.examples.straggler;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.vortex.driver.VortexLauncher;
import org.apache.reef.vortex.driver.VortexMasterConf;

import java.io.IOException;

/**
 * main function which launches Straggler application.
 */
public final class Straggler {
  private Straggler(){
  }

  public static void main(final String[] args) {
    final Tang tang = Tang.Factory.getTang();
    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();

    try {
      final CommandLine cl = new CommandLine(cb);
      cl.registerShortNameOfClass(Local.class)
          .registerShortNameOfClass(StragglerAddresses.class)
          .registerShortNameOfClass(HandleStraggler.class)
          .registerShortNameOfClass(NumTasklets.class)
          .registerShortNameOfClass(VortexMasterConf.MaxDuplicates.class)
          .processCommandLine(args);

      final Injector injector = tang.newInjector(cb.build());
      final boolean isLocal = injector.getNamedInstance(Local.class);
      final boolean handleStraggler = injector.getNamedInstance(HandleStraggler.class);

      if (isLocal) {
        VortexLauncher.launchLocal("Straggler", StragglerStart.class, handleStraggler, 16, 6144, 2, 2,
            cb.build());
      } else {
        VortexLauncher.launchYarn("Straggler", StragglerStart.class, handleStraggler, 16, 6144, 2, 2,
            cb.build());
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

  @NamedParameter(doc = "Address list of StragglerAddresses",
      short_name = "stragglers", default_value = "")
  public static final class StragglerAddresses implements Name<String> {
  }

  @NamedParameter(doc = "Whether to use straggler handling",
      short_name = "handle_straggler", default_value = "true")
  public static final class HandleStraggler implements Name<Boolean> {
  }

  @NamedParameter(doc = "Number of Tasklets to run",
      short_name = "num_tasklets", default_value = "6400")
  public static final class NumTasklets implements Name<Integer> {
  }
}
