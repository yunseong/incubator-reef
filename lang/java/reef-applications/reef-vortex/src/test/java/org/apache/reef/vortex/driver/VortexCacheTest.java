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
package org.apache.reef.vortex.driver;

import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.vortex.common.CacheKey;
import org.junit.Test;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Tests whether the VortexCache API works as expected at the Driver.
 */
public final class VortexCacheTest {
  private final VortexCache vortexCache;

  public VortexCacheTest() {
    try {
      vortexCache = Tang.Factory.getTang().newInjector().getInstance(VortexCache.class);
    } catch (InjectionException e) {
      throw new RuntimeException("InjectionException while injecting VortexCache");
    }
  }

  /**
   * Tests that the data of one type is inserted more than once.
   */
  @Test
  public void testDuplicateInsert() {
    final String data1 = new String("data1");
    final CacheKey<String> key1 = vortexCache.put(data1);

    final String data2 = new String("data2");
    final CacheKey<String> key2 = vortexCache.put(data2);

    assertNotEquals(key1, key2);
    assertNotEquals(vortexCache.get(key1), vortexCache.get(key2));
    assertEquals(data1, vortexCache.get(key1));
    assertEquals(data2, vortexCache.get(key2));
  }

  /**
   * Tests that the data is cached and retrieved correctly.
   */
  @Test
  public void testDataType() {
    checkCachedData(new String("string"));
    checkCachedData(new Integer(0));
    checkCachedData(new CustomData("string", 0));
  }

  /**
   * Checks whether the cached data is equal to the original data.
   * @param data The data to cache.
   * @param <T> The type of data.
   */
  private <T extends Serializable> void checkCachedData(final T data) {
    final CacheKey<T> cachedKey = vortexCache.put(data);
    final T cachedData = vortexCache.get(cachedKey);
    assertEquals(data, cachedData);
  }

  /**
   * Customized data type that consists of String and Integer.
   */
  final class CustomData implements Serializable {
    private final String stringValue;
    private final int intValue;

    private CustomData(final String stringValue, final int intValue) {
      this.stringValue = stringValue;
      this.intValue = intValue;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final CustomData that = (CustomData) o;

      if (intValue != that.intValue) {
        return false;
      }
      return stringValue.equals(that.stringValue);

    }

    @Override
    public int hashCode() {
      int result = stringValue.hashCode();
      result = 31 * result + intValue;
      return result;
    }
  }
}
