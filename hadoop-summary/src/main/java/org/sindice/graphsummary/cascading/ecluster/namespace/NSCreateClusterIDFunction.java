/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster.namespace;

import static org.sindice.core.analytics.util.AnalyticsCounters.TIME;
import static org.sindice.graphsummary.cascading.InstanceCounters.INSTANCE_ID;
import static org.sindice.graphsummary.cascading.InstanceCounters.NO_CLUSTER;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.BytesWritable;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
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
 * This {@link Function} creates a cluster identifier based on
 * the features outlined in {@link NSClusterSubAssembly}.
 */
public class NSCreateClusterIDFunction
extends SummaryBaseOperation<NSCreateClusterIDFunction.Context>
implements Function<NSCreateClusterIDFunction.Context> {

  private static final long serialVersionUID = 6811244682231345897L;

  private final boolean withIncoming;

  class Context {
    final Tuple       tuple     = Tuple.size(1);
    final Set<String> clusterId = new TreeSet<String>();
  }

  public NSCreateClusterIDFunction(Fields declaration, final boolean withIncoming) {
    super(2, declaration);
    this.withIncoming = withIncoming;
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
    // Process the outgoing edges
    final Statements spos = eout.iterateStatements();

    c.clusterId.clear();
    while (spos.getNext()) {
      final Value object = spos.getObject();

      if (AnalyticsClassAttributes.isClass(spos.getPredicate())) {
        // We got a type
        c.clusterId.add(RDFParser.getStringValue(object));
      } else {
        // We got a property
        c.clusterId.add(RDFParser.getStringValue(spos.getPredicate()));

        if (object instanceof URI) {
          final URI uri = (URI) object;
          c.clusterId.add(uri.getNamespace());
        }
      }
    }
    // Process the incoming edges
    if (withIncoming && ein != null) {
      ein.setFlowProcess(flowProcess);
      final Statements sposIn = ein.iterateStatements();

      while (sposIn.getNext()) {
        final URI object = (URI) sposIn.getObject();
        c.clusterId.add(object.getNamespace() + "-1");
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

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof NSCreateClusterIDFunction)) {
      return false;
    }
    if (!super.equals(object)) {
      return false;
    }

    final NSCreateClusterIDFunction func = (NSCreateClusterIDFunction) object;
    return withIncoming == func.withIncoming;
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = hash * 31 + (withIncoming ? 0 : 1);
    return hash;
  }

}
