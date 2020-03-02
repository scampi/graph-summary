/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.precision.connectivity;

import org.apache.hadoop.io.BytesWritable;
import org.sindice.analytics.benchmark.cascading.precision.BlankCluster;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.TupleEntry;

/**
 * This {@link Filter} lets through linksets which are connected to the blank node of the evaluated summary.
 * 
 * @see BlankCluster
 */
public class BlankFilter
extends BaseOperation<Void>
implements Filter<Void> {

  private static final long serialVersionUID = -3732071626186770754L;
  private final boolean     src;

  /**
   * If src is <code>true</code>, then keep only linksets which tail is the blank node. Otherwise, keep only those
   * which tail is not the blank node, but the head of the linkset is.
   */
  public BlankFilter(final boolean src) {
    super(3);
    this.src = src;
  }

  /**
   * Return <code>true</code> if the given node is the blank node.
   */
  private boolean isBlank(BytesWritable bw) {
    return bw.equals(BlankCluster.BLANK_CID);
  }

  @Override
  public boolean isRemove(FlowProcess flowProcess, FilterCall<Void> filterCall) {
    final TupleEntry args = filterCall.getArguments();
    final BytesWritable srcEval = (BytesWritable) args.getObject(0);
    final BytesWritable dstEval = (BytesWritable) args.getObject(1);
    final Object preds = args.getObject(2);

    if (preds == null && !isBlank(srcEval) && !isBlank(dstEval)) {
      throw new RuntimeException("Eval preds are null, but neither the src/dst-eval is the blank");
    }
    return src ? isBlank(srcEval) : !isBlank(srcEval) && isBlank(dstEval);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof BlankFilter)) {
      return false;
    }
    if (!super.equals(object)) {
      return false;
    }

    final BlankFilter blank = (BlankFilter) object;
    return blank.src != src;
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + (src ? 0 : 1);
    return hash;
  }

}
