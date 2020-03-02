/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster.fbisimulation;

    import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.sindice.core.analytics.util.Hash;
import org.sindice.core.analytics.util.ReusableByteArrayOutputStream;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Creates a new cluster identifier on 128 bits, which consists in the {@link Hash#getHash128(byte[]) hash} of the
 * concatenation of the cluster identifier, and the identifier of outgoing connected clusters.
 * <p>
 * This method takes two argument fields, the cluster identifier as a {@link BytesWritable} in position <code>0</code>,
 * and the identifier of connected clusters in a {@link Tuple} in position <code>1</code>.
 * <p>
 * The identifier of outgoing connected clusters are sorted in this Function,
 * in order to ensure that a unique cluster identifier is created.
 * <p>
 * If the set of outgoing connected clusters is empty or <code>null</code>, then the cluster identifier of
 * this entity is left unchanged.
 */
public class UpdateCidFunction
extends BaseOperation<UpdateCidFunction.Context>
implements Function<UpdateCidFunction.Context> {

  private static final long serialVersionUID = -732471672321252619L;

  class Context {
    final Tuple                         tuple = Tuple.size(1);
    final List<BytesWritable>           cids  = new ArrayList<BytesWritable>();
    final ReusableByteArrayOutputStream bytes = new ReusableByteArrayOutputStream(1024);
  }

  public UpdateCidFunction(final Fields dec) {
    super(2, dec);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall) {
    operationCall.setContext(new Context());
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Context> functionCall) {
    final Context c = functionCall.getContext();
    final TupleEntry args = functionCall.getArguments();
    final BytesWritable cid = (BytesWritable) args.getObject(0);
    final Tuple childrenCid = (Tuple) args.getObject(1);

    c.cids.clear();
    if (childrenCid == null || childrenCid.size() == 0) {
      c.tuple.set(0, cid);
    } else {
      try {
        // The set of cids must be kept in sorted order
        // this is to ensure a consistent cluster identifier
        for (Object o : childrenCid) {
          c.cids.add((BytesWritable) o);
        }
        Collections.sort(c.cids);

        c.bytes.write(cid.getBytes(), 0, cid.getLength());
        for (BytesWritable child : c.cids) {
          c.bytes.write(child.getBytes(), 0, child.getLength());
        }
        c.tuple.set(0, Hash.getHash128(c.bytes.getBytes(), 0, c.bytes.size()));
      } finally {
        c.bytes.reset();
      }
    }
    functionCall.getOutputCollector().add(c.tuple);
  }

}
