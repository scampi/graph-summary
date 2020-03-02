/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.precision.schema;

import org.sindice.graphsummary.cascading.properties.GetPropertiesGraph;
import org.sindice.graphsummary.cascading.properties.PropertiesCount;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * This {@link Function} encodes the properties information
 * from {@link GetPropertiesGraph} using a {@link Tuple}.
 */
public class PropertiesToTuple
extends BaseOperation<PropertiesToTuple.Context>
implements Function<PropertiesToTuple.Context> {

  private static final long serialVersionUID = -3752714651731753063L;

  class Context {
    final Tuple tuple = Tuple.size(1);
    final Tuple properties = new Tuple();
  }

  public PropertiesToTuple(final Fields fields) {
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

    final PropertiesCount props = (PropertiesCount) args.getObject(0);
    // Set of properties
    c.properties.clear();
    if (props != null) {
      for (long p : props.keySet()) {
        c.properties.add(p);
      }
    }
    c.tuple.set(0, c.properties);
    functionCall.getOutputCollector().add(c.tuple);
  }

}
