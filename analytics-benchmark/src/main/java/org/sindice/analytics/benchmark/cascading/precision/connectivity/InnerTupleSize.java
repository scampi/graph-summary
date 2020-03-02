/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.precision.connectivity;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * This {@link Function} outputs the number of elements in the {@link Tuple} at the position <code>0</code> of the
 * argument {@link Fields}.
 */
public class InnerTupleSize
extends BaseOperation<Tuple>
implements Function<Tuple> {

  private static final long serialVersionUID = -8214758110282844963L;

  public InnerTupleSize(Fields fields) {
    super(1, fields);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Tuple> operationCall) {
    operationCall.setContext(Tuple.size(1));
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Tuple> functionCall) {
    final TupleEntry args = functionCall.getArguments();
    final Tuple tuple = functionCall.getContext();

    tuple.set(0, ((Tuple) args.getObject(0)).size());
    functionCall.getOutputCollector().add(tuple);
  }

}
