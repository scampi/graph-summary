/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.precision.schema;

import org.apache.hadoop.io.BytesWritable;
import org.sindice.analytics.benchmark.cascading.precision.BlankCluster;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.TupleEntry;

/**
 * This {@link Filter} lets through linksets which tail is the blank node of the evaluated summary.
 * 
 * @see BlankCluster
 */
public class BlankFilter
extends BaseOperation<Void>
implements Filter<Void> {

  private static final long serialVersionUID = 4733022559433895216L;

  public BlankFilter() {
    super(1);
  }

  /**
   * Returns <code>true</code> if the given node is the blank node.
   */
  private boolean isBlank(BytesWritable bw) {
    return bw.equals(BlankCluster.BLANK_CID);
  }

  @Override
  public boolean isRemove(FlowProcess flowProcess, FilterCall<Void> filterCall) {
    final TupleEntry args = filterCall.getArguments();
    return isBlank((BytesWritable) args.getObject(0));
  }

}
