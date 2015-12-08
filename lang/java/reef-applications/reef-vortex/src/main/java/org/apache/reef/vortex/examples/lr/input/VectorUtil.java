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
package org.apache.reef.vortex.examples.lr.input;

import edu.snu.utils.DVector;
import edu.snu.utils.SVector;

/**
 * Created by v-yunlee on 12/4/2015.
 */
public final class VectorUtil {
  public static SVector toBreezeSparse(final SparseVector vector) {
    return new SVector(vector.getIndices(), vector.getValues(), vector.getDimension());
  }

  public static DVector toBreezeDense(final DenseVector vector) {
    return new DVector(vector.getData());
  }

  public static SparseVector fromBreeze(final SVector vector) {
    return new SparseVector(vector.data(), vector.index(), vector.length());
  }

  public static DenseVector fromBreeze(final DVector vector) {
    return new DenseVector(vector.data());
  }
}
