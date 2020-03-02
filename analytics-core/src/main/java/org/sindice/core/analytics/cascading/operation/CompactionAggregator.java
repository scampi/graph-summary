/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.operation;

import java.util.HashSet;
import java.util.Set;

import org.sindice.core.analytics.util.AnalyticsCounters;
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
 * This {@link Aggregator} keeps a set of {@link Comparable} elements and outputs a them into a nested {@link Tuple}.
 */
public class CompactionAggregator
extends BaseOperation<CompactionAggregator.Context>
implements Aggregator<CompactionAggregator.Context> {

  private static final long serialVersionUID = -7758838763014232985L;

  class Context {
    final Set<Comparable> set   = new HashSet<Comparable>();
    final Tuple           tuple = new Tuple((Object) new Tuple());
  }

  public CompactionAggregator(Fields fieldDeclaration) {
    super(1, fieldDeclaration);
  }

  @Override
  public void prepare(final FlowProcess flowProcess, final OperationCall<Context> operationCall) {
    operationCall.setContext(new Context());
  }

  public void start(final FlowProcess flowProcess, final AggregatorCall<Context> aggregatorCall) {
    aggregatorCall.getContext().set.clear();
    ((Tuple) aggregatorCall.getContext().tuple.getObject(0)).clear();
  }

  public void aggregate(final FlowProcess flowProcess, final AggregatorCall<Context> aggregatorCall) {
    final TupleEntry args = aggregatorCall.getArguments();
    final Context c = aggregatorCall.getContext();

    final Tuple objs = (Tuple) args.getObject(0);
    for (final Object o : objs) {
      c.set.add((Comparable) o);
    }
  }

  public void complete(final FlowProcess flowProcess, final AggregatorCall<Context> aggregatorCall) {
    final Context c = aggregatorCall.getContext();
    final Tuple nest = (Tuple) c.tuple.getObject(0);

    AnalyticsUtil.incrementByRange(flowProcess, AnalyticsCounters.GROUP, CompactionBy.class.getSimpleName() + "_set",
      c.set.size(), 10, 50, 100, 500, 1000, 5000, 10000, 20000, 50000, 100000);
    for (final Comparable o : c.set) {
      nest.add(o);
    }
    aggregatorCall.getOutputCollector().add(c.tuple);
  }

}
