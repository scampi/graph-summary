/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.properties;

import static org.sindice.core.analytics.util.AnalyticsCounters.TIME;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;
import static org.sindice.graphsummary.cascading.SummaryCounters.CLUSTERS;
import static org.sindice.graphsummary.cascading.SummaryCounters.SUMMARY_ID;

import org.apache.hadoop.io.BytesWritable;
import org.sindice.core.analytics.util.AnalyticsUtil;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * {@link Aggregator} of the {@link ClusterIDAggregateBy}.
 */
public class ClusterIDAggregate
extends BaseOperation<ClusterIDAggregate.Context>
implements Aggregator<ClusterIDAggregate.Context> {

  private static final long serialVersionUID = -2009908316103815408L;

  public class Context {
    final Tuple           tuple      = new Tuple();
    final PropertiesCount attributes = new PropertiesCount();
    final TypesCount      types      = new TypesCount();
  }

  public ClusterIDAggregate(Fields fieldDeclaration) {
    super(2, fieldDeclaration);
  }

  @Override
  public void prepare(FlowProcess flowProcess,
                      OperationCall<Context> operationCall) {
    operationCall.setContext(new Context());
  }

  public void start(FlowProcess flowProcess,
                    AggregatorCall<Context> aggregatorCall) {
    final Context c = aggregatorCall.getContext();

    c.attributes.clear();
    c.types.clear();
    c.tuple.clear();
    c.attributes.setFlowProcess(flowProcess);
    c.attributes.setClusterId((BytesWritable) aggregatorCall.getGroup().getObject("cluster-id"));
  }

  public void aggregate(FlowProcess flowProcess,
                        AggregatorCall<Context> aggregatorCall) {
    final long start = System.currentTimeMillis();

    final TupleEntry args = aggregatorCall.getArguments();
    final Context c = aggregatorCall.getContext();

    // Attributes
    final PropertiesCount properties = (PropertiesCount) args.getObject("properties");
    c.attributes.add(properties);
    // Types
    final TypesCount types = (TypesCount) args.getObject("types");
    c.types.add(types);

    flowProcess.increment(JOB_ID, TIME + this.getClass().getSimpleName(), System.currentTimeMillis() - start);
  }

  public void complete(FlowProcess flowProcess,
                       AggregatorCall<Context> aggregatorCall) {
    final long start = System.currentTimeMillis();
    final Context c = aggregatorCall.getContext();

    AnalyticsUtil.incrementByRange(flowProcess, JOB_ID, ClusterIDAggregate.class.getSimpleName() + " - attributes",
      c.attributes.size(), 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000);
    AnalyticsUtil.incrementByRange(flowProcess, JOB_ID, ClusterIDAggregate.class.getSimpleName() + " - types",
      c.types.size(), 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000);
    c.tuple.addAll(c.types, c.attributes);

    aggregatorCall.getOutputCollector().add(c.tuple);

    flowProcess.increment(SUMMARY_ID, CLUSTERS, 1);
    flowProcess.increment(JOB_ID, TIME + this.getClass().getSimpleName(), System.currentTimeMillis() - start);
  }

}
