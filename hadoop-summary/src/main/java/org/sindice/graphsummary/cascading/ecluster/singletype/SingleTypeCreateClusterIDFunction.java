/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster.singletype;

import static org.sindice.core.analytics.util.AnalyticsCounters.TIME;
import static org.sindice.graphsummary.cascading.InstanceCounters.INSTANCE_ID;
import static org.sindice.graphsummary.cascading.InstanceCounters.NO_CLUSTER;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import org.sindice.analytics.entity.EntityDescription;
import org.sindice.analytics.entity.EntityDescription.Statements;
import org.sindice.core.analytics.rdf.AnalyticsClassAttributes;
import org.sindice.graphsummary.cascading.SummaryBaseOperation;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Function to create the CID. Here we create one CID by types.
 * 
 * @author artbau
 */
public class SingleTypeCreateClusterIDFunction
extends SummaryBaseOperation<Tuple>
implements Function<Tuple> {

  public SingleTypeCreateClusterIDFunction(Fields fieldDeclaration) {
    super(1, fieldDeclaration);
  }

  @Override
  public void prepare(FlowProcess flowProcess,
                      OperationCall<Tuple> operationCall) {
    super.prepare(flowProcess, operationCall);
    operationCall.setContext(Tuple.size(1));
  }

  @Override
  public void operate(FlowProcess flowProcess,
                      FunctionCall<Tuple> functionCall) {
    final long start = System.currentTimeMillis();

    final Tuple tuple = functionCall.getContext();
    final TupleEntry args = functionCall.getArguments();
    final EntityDescription eout = (EntityDescription) args.getObject("spo-out");
    boolean hasType = false;

    eout.setFlowProcess(flowProcess);
    final Statements spos = eout.iterateStatements();
    while (spos.getNext()) {
      if (AnalyticsClassAttributes.isClass(spos.getPredicate())) {
        // We got a type
        hasType = true;
        tuple.set(0, spos.getObjectHash128());
        functionCall.getOutputCollector().add(tuple);
      }
    }

    if (!hasType) {
      flowProcess.increment(INSTANCE_ID, NO_CLUSTER, 1);
    }

    flowProcess.increment(JOB_ID, TIME + "CreateClusterIDFunction", System.currentTimeMillis() - start);
  }

}
