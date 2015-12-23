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
package org.apache.reef.io.serialization;

import java.io.IOException;

/**
 * Interface for serialization routines that translate back and forth between
 * byte arrays with low latency. (Contrast to Serializer, Deserializer, which
 * optimize for file size and throughput.)
 *
 * @param <T> The type of the objects (de-)serialized
 */
public interface Codec<T> {

  /**
   * Encodes the given object into a Byte Array.
   *
   * @param obj
   * @return a byte[] representation of the object
   */
  byte[] encode(T obj) throws IOException;

  /**
   * Decodes the given byte array into an object.
   *
   * @param buf
   * @return the decoded object
   */
  T decode(byte[] buf) throws IOException;
}
