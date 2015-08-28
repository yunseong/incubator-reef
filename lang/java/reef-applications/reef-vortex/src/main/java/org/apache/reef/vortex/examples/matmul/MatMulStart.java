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
package org.apache.reef.vortex.examples.matmul;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.reef.vortex.api.VortexFuture;
import org.apache.reef.vortex.api.VortexStart;
import org.apache.reef.vortex.api.VortexThreadPool;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Random;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MatMul User Code Example.
 */
final class MatMulStart implements VortexStart {
  private static final Logger LOG = Logger.getLogger(MatMulStart.class.getName());
  private static final int MATRIX_WIDTH_AND_HEIGHT = 1000;
  private static final int DIVIDE_FACTOR = 10;

  @Inject
  private MatMulStart() {
  }

  /**
   * Perform a simple vector multiplication on Vortex.
   */
  @Override
  public void start(final VortexThreadPool vortexThreadPool) {
    // Create Matrix A and B(=A')
    final Random random = new Random();
    final Vector<Double> vector = new Vector<>();
    for (int i = 0; i < MATRIX_WIDTH_AND_HEIGHT; i++) {
      vector.add(random.nextDouble());
    }
    final ArrayList<Vector<Double>> matrix = new ArrayList<>();
    for (int i = 0; i < MATRIX_WIDTH_AND_HEIGHT; i++) {
      matrix.add(vector);
    }

    // Create sub-matrix of A (Because every row in A is same we create just one sub-matrix)
    final ArrayList<Vector<Double>> subMatrix = new ArrayList<>();
    for (int i = 0; i < MATRIX_WIDTH_AND_HEIGHT/DIVIDE_FACTOR; i++) {
      subMatrix.add(vector);
    }

    // Measure job finish time starting from here..
    final double start = System.currentTimeMillis();

    // Submit task(s)
    final ArrayList<VortexFuture<ArrayList<Vector<Double>>>> futures = new ArrayList<>();
    final MatMulFunction matMulFunction = new MatMulFunction();
    for (int i = 0; i < DIVIDE_FACTOR; i++) {
      // First type of the pair is normal matrix
      // Second type of the pair is tranposed matrix (just for convenience...)
      futures.add(vortexThreadPool.submit(matMulFunction, new ImmutablePair<>(subMatrix, matrix)));
    }

    // Print the resulting matrix
    try {
      for (final VortexFuture<ArrayList<Vector<Double>>> future : futures) {
        for (final Vector<Double> v : future.get()) {
          System.out.println(v);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    LOG.log(Level.INFO, "MATRIX WIDTH_AND_HEIGHT " + MATRIX_WIDTH_AND_HEIGHT);
    LOG.log(Level.INFO, "MATRIX DIVIDE_FACTOR " + DIVIDE_FACTOR);
    LOG.log(Level.INFO, "Job Finish Time: " + (System.currentTimeMillis() - start));
  }
}
