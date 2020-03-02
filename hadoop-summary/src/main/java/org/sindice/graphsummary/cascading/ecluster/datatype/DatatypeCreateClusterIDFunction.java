/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster.datatype;

import static org.sindice.core.analytics.util.AnalyticsCounters.TIME;
import static org.sindice.graphsummary.cascading.InstanceCounters.INSTANCE_ID;
import static org.sindice.graphsummary.cascading.InstanceCounters.NO_CLUSTER;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.BytesWritable;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.sindice.analytics.entity.AnalyticsLiteral;
import org.sindice.analytics.entity.EntityDescription;
import org.sindice.analytics.entity.EntityDescription.Statements;
import org.sindice.core.analytics.rdf.AnalyticsClassAttributes;
import org.sindice.core.analytics.rdf.RDFParser;
import org.sindice.core.analytics.util.Hash;
import org.sindice.graphsummary.cascading.InstanceCounters;
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
 * the features outlined in {@link DatatypeClusterSubAssembly}.
 * <p>
 * A literal without a datatype has by default the {@value #XSD_STRING} datatype.
 * <p>
 * The Hadoop counter <b>DATATYPE</b> in the group {@link InstanceCounters#INSTANCE_ID}
 * counts the number of datatype which are different from {@value #XSD_STRING}.
 */
public class DatatypeCreateClusterIDFunction
extends SummaryBaseOperation<DatatypeCreateClusterIDFunction.Context>
implements Function<DatatypeCreateClusterIDFunction.Context> {

  private static final long  serialVersionUID = 6811244682231345897L;
  public static final String XSD_STRING       = "http://www.w3.org/2001/XMLSchema#string";

  class Context {
    final Tuple       tuple     = Tuple.size(1);
    final Set<String> clusterId = new TreeSet<String>();
  }

  public DatatypeCreateClusterIDFunction(Fields declaration) {
    super(1, declaration);
  }

  @Override
  public void prepare(FlowProcess flowProcess,
                      OperationCall<Context> operationCall) {
    super.prepare(flowProcess, operationCall);
    operationCall.setContext(new Context());
  }

  /**
   * Returns the datatype of this literal.
   * By default, an {@link AnalyticsLiteral} has the {@value #XSD_STRING} datatype.
   */
  private String getDatatype(FlowProcess flowProcess, AnalyticsLiteral lit) {
    final URI dt = lit.getDatatype();
    if (dt == null) {
      return XSD_STRING;
    }
    final String datatype = dt.stringValue();
    flowProcess.increment(INSTANCE_ID, "DATATYPE", datatype.equals(XSD_STRING) ? 0 : 1);
    return datatype;
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
      final Value object = spos.getObject();

      if (AnalyticsClassAttributes.isClass(spos.getPredicate())) {
        // We got a type
        c.clusterId.add(RDFParser.getStringValue(object));
      } else {
        // We got a property
        c.clusterId.add(RDFParser.getStringValue(spos.getPredicate()));

        if (object instanceof Literal) {
          // if no datatype attached, use the default XSD_STRING
          final String dt = getDatatype(flowProcess, (AnalyticsLiteral) object);
          c.clusterId.add(dt);
        }
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
