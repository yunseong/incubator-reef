package org.apache.reef.vortex.examples.lr.input;

import edu.snu.utils.SVector;

/**
 * Created by v-yunlee on 12/4/2015.
 */
public final class VectorUtil {
  public static SVector toBreezeSparse(final SparseVector vector) {
    return new SVector(vector.getIndices(), vector.getValues(), vector.getDimension());
  }

  public static DVector toBreezeSparse(final DenseVector vector) {
    return new DVector(vector.getData());
  }

}
