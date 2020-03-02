/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.statistics;

import org.sindice.graphsummary.cascading.properties.PropertiesCount;
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
 * This {@link Function} counts the number of edges in a summary.
 */
public class CountEdgesFunction
extends BaseOperation<Tuple>
implements Function<Tuple> {

  public CountEdgesFunction(final Fields fields) {
    super(2, fields);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Tuple> operationCall) {
    operationCall.setContext(Tuple.size(1));
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Tuple> functionCall) {
    final TupleEntry args = functionCall.getArguments();
    final Tuple tuple = functionCall.getContext();

    final TypesCount types = (TypesCount) args.getObject("types");
    final PropertiesCount properties = (PropertiesCount) args.getObject("properties");

    tuple.set(0, types.size() + properties.size());
    functionCall.getOutputCollector().add(tuple);
  }

}
