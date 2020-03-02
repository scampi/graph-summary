/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.precision;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * This {@link Function} takes two {@link Fields} as argument, and outputs the first if the second is <code>null</code>,
 * and the second otherwise.
 */
public class BlankSwitch
extends BaseOperation<Tuple>
implements Function<Tuple> {

  private static final long serialVersionUID = 8391025584191999142L;

  public BlankSwitch(final Fields fields) {
    super(2, fields);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Tuple> operationCall) {
    operationCall.setContext(Tuple.size(1));
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Tuple> functionCall) {
    final Tuple tuple = functionCall.getContext();
    final TupleEntry args = functionCall.getArguments();

    if (args.getObject(1) == null) {
      tuple.set(0, args.getObject(0));
    } else {
      tuple.set(0, args.getObject(1));
    }
    functionCall.getOutputCollector().add(tuple);
  }

}
