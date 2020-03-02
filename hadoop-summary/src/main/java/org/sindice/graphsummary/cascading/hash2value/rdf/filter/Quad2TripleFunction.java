/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.hash2value.rdf.filter;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * This {@link Function} removes the context value from the quad, outputting a triple.
 */
public class Quad2TripleFunction
extends BaseOperation<Tuple>
implements Function<Tuple> {

  private static final long serialVersionUID = 598337566244750092L;

  public Quad2TripleFunction(final Fields fields) {
    super(1, fields);
  }

  @Override
  public void prepare(FlowProcess flowProcess,
                      OperationCall<Tuple> operationCall) {
    operationCall.setContext(Tuple.size(1));
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Tuple> functionCall) {
    final TupleEntry args = functionCall.getArguments();
    final String quad = args.getString("quad");

    final int ind = quad.lastIndexOf('<');
    if (ind == -1) {
      throw new RuntimeException("Malformated quad. Unable to get context from : " + quad);
    }
    final Tuple tuple = functionCall.getContext();
    tuple.set(0, quad.substring(0, ind) + ".");
    functionCall.getOutputCollector().add(tuple);
  }

}
