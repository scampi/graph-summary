/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.precision.connectivity;

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
import cascading.tuple.Tuples;

/**
 * This {@link Function} computes the number of true and false positive edges.
 */
public class ConnectedTFPositive
extends BaseOperation<ConnectedTFPositive.Context>
implements Function<ConnectedTFPositive.Context> {

  private static final long serialVersionUID = 1868705410578202156L;

  class Context {
    final Tuple tuple = Tuple.size(2);
    final Set<Long> goldSet = new HashSet<Long>();
  }

  public ConnectedTFPositive(final Fields fields) {
    super(fields);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall) {
    operationCall.setContext(new Context());
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Context> functionCall) {
    final TupleEntry args = functionCall.getArguments();
    final Context c = functionCall.getContext();

    final Tuple pGold = (Tuple) args.getObject("predicates-gold");
    final Tuple pEval = (Tuple) args.getObject("predicates-eval");

    long truePos = 0;
    long falsePos = 0;

    if (pGold.size() > pEval.size()) {
      throw new RuntimeException("Got more Golds preds than Eval preds");
    }
    // Get the gold predicates
    c.goldSet.clear();
    for (Object o : pGold) {
      c.goldSet.add(Tuples.toLong(o));
    }
    // True/False positive count
    for (Object o : pEval) {
      if (c.goldSet.contains(Tuples.toLong(o))) {
        truePos++;
      } else {
        falsePos++;
      }
    }

    c.tuple.set(0, truePos);
    c.tuple.set(1, falsePos);
    functionCall.getOutputCollector().add(c.tuple);
  }

}
