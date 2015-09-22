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
package org.apache.reef.vortex.common;

import java.io.Serializable;

/**
 * Key used to get the data from the Worker cache
 * Users should assign unique name to distinguish the keys.
 */
public final class CacheKey<T extends Serializable> implements Serializable {
  private final String name;
  public CacheKey(final String name) {
    this.name = name;
  }

  /**
   * @return Name of the key.
   */
  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return "CacheKey{name='" + name + "'}";
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final CacheKey<?> cacheKey = (CacheKey<?>) o;

    return name.equals(cacheKey.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }
}
