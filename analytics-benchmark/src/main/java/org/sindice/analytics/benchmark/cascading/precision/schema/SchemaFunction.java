/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.precision.schema;

import java.util.HashSet;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * This {@link Function} computes the number of false and true positive edges.
 */
public class SchemaFunction
extends BaseOperation<SchemaFunction.Context>
implements Function<SchemaFunction.Context> {

  private static final long serialVersionUID = -681829783486431218L;

  class Context {
    final Tuple     tuple = Tuple.size(2);
    final Set<Long> set   = new HashSet<Long>();
  }

  public SchemaFunction(final Fields fields) {
    super(2, fields);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall) {
    operationCall.setContext(new Context());
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Context> functionCall) {
    final TupleEntry args = functionCall.getArguments();
    final Context c = functionCall.getContext();

    final Tuple gold = (Tuple) args.getObject(0);
    final Tuple eval = (Tuple) args.getObject(1);

    c.set.clear();
    for (int i = 0; i < gold.size(); i++) {
      c.set.add(gold.getLong(i));
    }
    final long goldset = (long) c.set.size();
    for (int i = 0; i < eval.size(); i++) {
      c.set.add(eval.getLong(i));
    }
    final long totalset = (long) c.set.size();

    if (totalset < goldset) {
      throw new RuntimeException("The poweverset of the eval cluster is bigger " +
          "than the power set of the gold clusters!");
    }
    c.tuple.set(0, goldset);
    c.tuple.set(1, totalset - goldset);
    functionCall.getOutputCollector().add(c.tuple);
  }

}
