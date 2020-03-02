/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster.types;

import static org.sindice.core.analytics.util.AnalyticsCounters.TIME;
import static org.sindice.graphsummary.cascading.InstanceCounters.INSTANCE_ID;
import static org.sindice.graphsummary.cascading.InstanceCounters.NO_CLUSTER;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.BytesWritable;
import org.sindice.analytics.entity.EntityDescription;
import org.sindice.analytics.entity.EntityDescription.Statements;
import org.sindice.core.analytics.rdf.AnalyticsClassAttributes;
import org.sindice.core.analytics.rdf.RDFParser;
import org.sindice.core.analytics.util.Hash;
import org.sindice.graphsummary.cascading.SummaryBaseOperation;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Function to create the CID. Here we create the cluster from the name of all
 * the types aggregated.
 * 
 * @author Pierre Bailly-Ferry <pierre.bailly@deri.org>
 */
public class TypesCreateClusterIDFunction
extends SummaryBaseOperation<TypesCreateClusterIDFunction.Context>
implements Function<TypesCreateClusterIDFunction.Context> {

  private static final long serialVersionUID = -7378892695175341145L;

  class Context {
    final Tuple       tuple     = Tuple.size(1);
    final Set<String> clusterId = new TreeSet<String>();
  }

  public TypesCreateClusterIDFunction(Fields declaration) {
    super(1, declaration);
  }

  @Override
  public void prepare(FlowProcess flowProcess,
                      OperationCall<Context> operationCall) {
    super.prepare(flowProcess, operationCall);
    operationCall.setContext(new Context());
  }

  @Override
  public void operate(FlowProcess flowProcess,
                      FunctionCall<Context> functionCall) {
    final long start = System.currentTimeMillis();

    final Context c = functionCall.getContext();
    final TupleEntry args = functionCall.getArguments();
    final EntityDescription eout = (EntityDescription) args.getObject("spo-out");

    eout.setFlowProcess(flowProcess);
    c.clusterId.clear();
    final Statements spos = eout.iterateStatements();
    while (spos.getNext()) {
      if (AnalyticsClassAttributes.isClass(spos.getPredicate())) {
        c.clusterId.add(RDFParser.getStringValue(spos.getObject()));
      }
    }
    if (c.clusterId.size() == 0) {
      flowProcess.increment(INSTANCE_ID, NO_CLUSTER, 1);
      return;
    }

    final BytesWritable cid = Hash.getHash128(c.clusterId);

    c.tuple.set(0, cid);
    functionCall.getOutputCollector().add(c.tuple);

    flowProcess.increment(JOB_ID, TIME + "CreateClusterIDFunction", System.currentTimeMillis() - start);
  }

}
