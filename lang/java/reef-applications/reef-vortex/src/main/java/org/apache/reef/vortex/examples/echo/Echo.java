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
package org.apache.reef.vortex.examples.echo;

import org.apache.reef.tang.Tang;
import org.apache.reef.vortex.driver.VortexLauncher;

/**
 * User's main function.
 */
public final  class Echo {
  private Echo() {
  }

  /**
   * Launch the vortex job, passing appropriate arguments.
   */
  public static void main(final String[] args) {
    VortexLauncher.launchLocal("Vortex_Example_Echo", EchoStart.class, 1, 1024, 8, 2000,
        Tang.Factory.getTang().newConfigurationBuilder().build());
  }
}
