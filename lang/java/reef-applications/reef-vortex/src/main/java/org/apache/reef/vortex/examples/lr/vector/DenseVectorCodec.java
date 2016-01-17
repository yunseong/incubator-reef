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
package org.apache.reef.vortex.examples.lr.vector;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.reef.io.serialization.Codec;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Codec to serialize/deserialize DenseVector.
 */
public final class DenseVectorCodec implements Codec<DenseVector> {

  @Override
  public byte[] encode(final DenseVector obj) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      try (DataOutputStream dos = new DataOutputStream(baos)) {
        final float[] data = obj.getData();
        final int length = data.length;
        dos.writeInt(length);
        for (int i = 0; i < length; i++) {
          dos.writeFloat(data[i]);
        }
        return baos.toByteArray();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DenseVector decode(final byte[] buf) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(buf)) {
      try (DataInputStream dais = new DataInputStream(bais)) {
        final int length = dais.readInt();
        final float[] data = new float[length];
        for (int i = 0; i < length; i++) {
          data[i] = dais.readFloat();
        }
        return new DenseVector(data);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}