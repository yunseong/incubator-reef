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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.reef.io.serialization.Codec;

import java.io.IOException;

/**
 * Created by v-yunlee on 1/19/2016.
 */
public class ALSFunctionOutputCodec implements Codec<ResultVector[]> {
  @Override
  public byte[] encode(final ResultVector[] obj) {
    final Kryo kryo = new Kryo();
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      try (final Output output = new Output(baos)) {
        kryo.writeObject(output, obj);
      }
      return baos.toByteArray();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ResultVector[] decode(final byte[] buf) {
    final Kryo kryo = new Kryo();
    try (final Input input = new Input(buf)) {
      return kryo.readObject(input, ResultVector[].class);
    }
  }
}
