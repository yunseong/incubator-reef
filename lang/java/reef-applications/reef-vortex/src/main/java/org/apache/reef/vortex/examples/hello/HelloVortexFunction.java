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
package org.apache.reef.vortex.examples.hello;

import org.apache.reef.io.serialization.Codec;
import org.apache.reef.vortex.api.VortexFunction;
import org.apache.reef.vortex.util.VoidCodec;

/**
 * Prints to stdout.
 */
public final class HelloVortexFunction implements VortexFunction<Void, Void> {
  private static final Codec<Void> CODEC =  new VoidCodec();
  /**
   * Prints to stdout.
   */
  @Override
  public Void call(final Void input) throws Exception {
    System.out.println("Hello, Vortex!");
    return null;
  }

  @Override
  public Codec<Void> getInputCodec() {
    return CODEC;
  }

  @Override
  public Codec<Void> getOutputCodec() {
    return CODEC;
  }
}
