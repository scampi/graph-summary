/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.hash2value.rdf.filter;

import org.sindice.graphsummary.cascading.hash2value.rdf.DataGraphSummaryVocab;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * This {@link Function} add a quad value to the triple. The context
 * is equal to {@link DataGraphSummaryVocab#GRAPH_SUMMARY_GRAPH}.
 */
public class Triple2QuadFunction
extends BaseOperation<Tuple>
implements Function<Tuple> {

  private static final long serialVersionUID = 598337566244750092L;

  public Triple2QuadFunction(final Fields fields) {
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
    final String triple = args.getString("quad");

    final int lastWS = triple.lastIndexOf(' ');
    if (lastWS == -1) {
      throw new RuntimeException("Malformated triple: " + triple);
    }
    final Tuple tuple = functionCall.getContext();
    tuple.set(0, triple.replaceFirst(" .$", " <" + DataGraphSummaryVocab.GRAPH_SUMMARY_GRAPH + "> ."));
    functionCall.getOutputCollector().add(tuple);
  }

}
