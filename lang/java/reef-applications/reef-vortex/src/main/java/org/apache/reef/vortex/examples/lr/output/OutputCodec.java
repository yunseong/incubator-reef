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
package org.apache.reef.vortex.examples.lr.output;

import org.apache.reef.io.serialization.Codec;
import org.apache.reef.vortex.examples.lr.vector.DenseVector;
import org.apache.reef.vortex.examples.lr.vector.DenseVectorCodec;

import java.io.*;

/**
 * Codec to serialize/deserialize DenseVector.
 */
public class OutputCodec implements Codec<GradientFunctionOutput> {
  private static final DenseVectorCodec DENSE_VECTOR_CODEC = new DenseVectorCodec();
  @Override
  public byte[] encode(final GradientFunctionOutput obj) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      try (DataOutputStream dos = new DataOutputStream(baos)) {
        dos.writeInt(obj.getCountTotal());
        dos.writeInt(obj.getCountPositive());

        final byte[] serializedPartialGradient = DENSE_VECTOR_CODEC.encode(obj.getPartialGradient());
        dos.writeInt(serializedPartialGradient.length);
        dos.write(serializedPartialGradient);
      }
      return baos.toByteArray();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public GradientFunctionOutput decode(final byte[] buf) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(buf)) {
      try (DataInputStream dais = new DataInputStream(bais)) {
        final int count = dais.readInt();
        final int positiveCount = dais.readInt();

        final int length = dais.readInt();
        final byte[] serializedPartialGradient = new byte[length];
        final int numRead = dais.read(serializedPartialGradient, 0, length);
        assert numRead == length;
        final DenseVector partialGradient = DENSE_VECTOR_CODEC.decode(serializedPartialGradient);

        return new GradientFunctionOutput(partialGradient, positiveCount, count);

      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
