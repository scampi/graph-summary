/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster.fbisimulation.adjacency;

import static org.sindice.core.analytics.util.AnalyticsCounters.TIME;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;
import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * This {@link Aggregator} collects a {@link Tuple} with a <code>null</code> value if the entity
 * is a sink, and with <code>0</code> otherwise.
 */
public class SinkAggregate
extends BaseOperation<Tuple>
implements Aggregator<Tuple> {

  private static final long serialVersionUID = -3900874977420493352L;

  public SinkAggregate(Fields fieldDeclaration) {
    super(1, fieldDeclaration);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Tuple> operationCall) {
    operationCall.setContext(Tuple.size(1));
  }

  public void start(final FlowProcess flowProcess, final AggregatorCall<Tuple> aggregatorCall) {
    aggregatorCall.getContext().set(0, 0);
  }

  public void aggregate(final FlowProcess flowProcess, final AggregatorCall<Tuple> aggregatorCall) {
    final long start = System.currentTimeMillis();

    final TupleEntry args = aggregatorCall.getArguments();

    if (args.getBoolean(0)) {
      aggregatorCall.getContext().set(0, null);
    }

    flowProcess.increment(JOB_ID, TIME + this.getClass().getSimpleName(), System.currentTimeMillis() - start);
  }

  public void complete(final FlowProcess flowProcess, final AggregatorCall<Tuple> aggregatorCall) {
    final long start = System.currentTimeMillis();

    // if null, this entity is a sink
    aggregatorCall.getOutputCollector().add(aggregatorCall.getContext());

    flowProcess.increment(JOB_ID, TIME + this.getClass().getSimpleName(), System.currentTimeMillis() - start);
  }

}
