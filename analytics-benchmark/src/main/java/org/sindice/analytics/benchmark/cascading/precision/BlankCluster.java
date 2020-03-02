/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.precision;

import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import org.apache.hadoop.io.BytesWritable;
import org.sindice.core.analytics.util.Hash;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * This {@link Function} assign an entity to the {@link #BLANK_CID} if it has no mapping in the evaluated summary.
 */
public class BlankCluster
extends BaseOperation<Tuple>
implements Function<Tuple> {

  private static final long         serialVersionUID = -6751913648379125976L;
  public static final BytesWritable BLANK_CID        = Hash.getHash128("###blank-equivalence-class###");

  public BlankCluster(final Fields fields) {
    super(1, fields);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Tuple> operationCall) {
    operationCall.setContext(Tuple.size(1));
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Tuple> functionCall) {
    final Tuple tuple = functionCall.getContext();
    final Object arg = functionCall.getArguments().getObject(0);

    if (arg == null) {
      flowProcess.increment(JOB_ID, "BLANK_CID", 1);
      tuple.set(0, BLANK_CID);
    } else {
      tuple.set(0, arg);
    }
    functionCall.getOutputCollector().add(tuple);
  }

}
