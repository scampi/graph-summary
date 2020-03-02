/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster.fbisimulation.adjacency;

import static org.sindice.core.analytics.util.AnalyticsCounters.TIME;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import org.apache.hadoop.io.BytesWritable;

import cascading.flow.FlowProcess;
import cascading.pipe.assembly.AggregateBy.Functor;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * This {@link Functor} collects a tuple with <code>true</code> if this entity is a sink.
 */
public class SinkAggregatePartials
implements Functor {

  private static final long serialVersionUID = -6999276701559286491L;
  private final Fields      declaredFields;

  public SinkAggregatePartials(Fields declaredFields) {
    this.declaredFields = declaredFields;

    if (declaredFields.size() != 1) {
      throw new IllegalArgumentException("declared fields can only have 1 fields, got: " + declaredFields);
    }
  }

  @Override
  public Tuple aggregate(final FlowProcess flowProcess, final TupleEntry args, Tuple context) {
    final long start = System.currentTimeMillis();

    if (context == null) {
      context = new Tuple(true);
    }

    final BytesWritable entity = (BytesWritable) args.getObject(0);
    if (entity != null) { // at least 1 outgoing entity appears as a subject
      context.setBoolean(0, false);
    }

    flowProcess.increment(JOB_ID, TIME + this.getClass().getSimpleName(), System.currentTimeMillis() - start);
    return context;
  }

  @Override
  public Tuple complete(final FlowProcess flowProcess, final Tuple context) {
    return context;
  }

  @Override
  public Fields getDeclaredFields() {
    return declaredFields;
  }

}
