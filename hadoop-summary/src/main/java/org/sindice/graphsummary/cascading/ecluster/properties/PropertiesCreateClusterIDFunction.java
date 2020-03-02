/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster.properties;

import static org.sindice.core.analytics.util.AnalyticsCounters.TIME;
import static org.sindice.graphsummary.cascading.InstanceCounters.INSTANCE_ID;
import static org.sindice.graphsummary.cascading.InstanceCounters.NO_CLUSTER;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.BytesWritable;
import org.sindice.analytics.entity.EntityDescription;
import org.sindice.analytics.entity.EntityDescription.Statements;
import org.sindice.core.analytics.rdf.RDFParser;
import org.sindice.core.analytics.util.Hash;
import org.sindice.graphsummary.cascading.SummaryBaseOperation;
import org.sindice.graphsummary.cascading.ecluster.ClusterSubAssembly;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * This {@link ClusterSubAssembly} cluster entities based on the following attribute feature, i.e., the label of the
 * predicates associated to the entity.
 */
public class PropertiesCreateClusterIDFunction
extends SummaryBaseOperation<PropertiesCreateClusterIDFunction.Context>
implements Function<PropertiesCreateClusterIDFunction.Context> {

  private static final long serialVersionUID = -7549955250306904866L;

  class Context {
    final Tuple       tuple     = Tuple.size(1);
    final Set<String> clusterId = new TreeSet<String>();
  }

  public PropertiesCreateClusterIDFunction(Fields declaration) {
    super(2, declaration);
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

    final EntityDescription ein = (EntityDescription) args.getObject("spo-in");
    final EntityDescription eout = (EntityDescription) args.getObject("spo-out");

    eout.setFlowProcess(flowProcess);
    c.clusterId.clear();
    if (ein != null) {
      ein.setFlowProcess(flowProcess);
      buildClusterID(c.clusterId, ein);
    }
    buildClusterID(c.clusterId, eout);
    if (c.clusterId.size() == 0) {
      flowProcess.increment(INSTANCE_ID, NO_CLUSTER, 1);
      return;
    }

    final BytesWritable cid = Hash.getHash128(c.clusterId);

    c.tuple.set(0, cid);
    functionCall.getOutputCollector().add(c.tuple);

    flowProcess.increment(JOB_ID, TIME + "CreateClusterIDFunction", System.currentTimeMillis() - start);
  }

  /**
   * Get the predicates label of the entity.
   */
  private void buildClusterID(final Set<String> clusterId, final EntityDescription e) {
    final Statements spos = e.iterateStatements();

    while (spos.getNext()) {
      clusterId.add(RDFParser.getStringValue(spos.getPredicate()));
    }
  }

}
