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
package org.apache.reef.vortex.examples.lr;

import org.apache.reef.vortex.api.VortexFunction;

import java.util.ArrayList;
import java.util.Vector;

/**
 * Outputs the gradient.
 */
final class GradientFunction
    implements VortexFunction<LRInputNotCached, Vector<Double>> {
//    implements VortexFunction<LRInputHalfCached, Vector<Double>> {
//    implements VortexFunction<LogisticRegressionInput, Vector<Double>> {
  /**
   * Outputs the gradient.
   */
  @Override
    public Vector<Double> call(final LRInputNotCached lrInput) throws Exception {
//    public Vector<Double> call(final LRInputHalfCached lrInput) throws Exception {
//  public Vector<Double> call(final LogisticRegressionInput lrInput) throws Exception {
    final Vector<Double> parameterVector = lrInput.getParameterVector();
    final ArrayList<Vector<Double>> trainingData = lrInput.getTrainingData();

    // Initial zero-valued
    final Vector<Double> gradientResult = new Vector<>();
    for (int i = 0; i < parameterVector.size(); i++) {
      gradientResult.add(0.0);
    }

    // Get the gradient
    for (final Vector<Double> instance : trainingData) {
      System.out.println("gradient: " + gradientResult);

      // Convert [x1, x2, ... , xn, y] --> [x0(=1), x1, x2, ... , xn]
      final Vector<Double> xVector = new Vector<>();
      xVector.add(1.0);
      for (int j = 0; j < instance.size()-1; j++) {
        xVector.add(instance.get(j));
      }

      // Get the gradient
      final double hypothesis = getHypothesis(parameterVector, xVector);
      final double y = instance.get(instance.size() - 1);
      /*
      System.out.println("instance: " + instance);
      System.out.println("xVector: " + xVector);
      System.out.println("hypothesis: " + hypothesis);
      System.out.println("y: " + y);
      */

      final double multiplier =  hypothesis - y;
      for (int i = 0; i < parameterVector.size(); i++) {
        gradientResult.setElementAt(gradientResult.get(i) + (multiplier * instance.get(i)), i);
      }
    }

    return gradientResult;
  }

  private double getHypothesis(final Vector<Double> parameterVector, final Vector<Double> xVector) {
    final double exponent = -1.0 * multiplyAndAddVectors(parameterVector, xVector);
    return 1 / (1 + Math.pow(Math.E, exponent));
  }

  private double multiplyAndAddVectors(final Vector<Double> aVector, final Vector<Double> bVector) {
    assert(!aVector.isEmpty());
    assert(aVector.size() == bVector.size());
    double result = 0.0;
    final int size = aVector.size();
    for (int i = 0; i < size; i++) {
      result += (aVector.get(i) * bVector.get(i));
    }
    return result;
  }
}
