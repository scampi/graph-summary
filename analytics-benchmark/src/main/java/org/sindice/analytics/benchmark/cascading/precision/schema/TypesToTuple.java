/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.precision.schema;

import org.sindice.graphsummary.cascading.properties.GetPropertiesGraph;
import org.sindice.graphsummary.cascading.properties.TypesCount;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * This {@link Function} encodes the type information
 * from {@link GetPropertiesGraph} using a {@link Tuple}.
 */
public class TypesToTuple
extends BaseOperation<TypesToTuple.Context>
implements Function<TypesToTuple.Context> {

  private static final long serialVersionUID = -3752714651731753063L;

  class Context {
    final Tuple tuple = Tuple.size(1);
    final Tuple types = new Tuple();
  }

  public TypesToTuple(final Fields fields) {
    super(1, fields);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall) {
    operationCall.setContext(new Context());
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Context> functionCall) {
    final TupleEntry args = functionCall.getArguments();
    final Context c = functionCall.getContext();

    final TypesCount types = (TypesCount) args.getObject(0);

    // Set of types
    c.types.clear();
    if (types != null) {
      for (long t : types.keySet()) {
        c.types.add(t);
      }
    }
    c.tuple.set(0, c.types);
    functionCall.getOutputCollector().add(c.tuple);
  }

}
