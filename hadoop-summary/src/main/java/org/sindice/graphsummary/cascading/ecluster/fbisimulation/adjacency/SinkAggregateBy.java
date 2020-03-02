/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster.fbisimulation.adjacency;

import cascading.pipe.assembly.AggregateBy;
import cascading.tuple.Fields;

/**
 * This {@link AggregateBy} detects entities that are sinks.
 * <p>
 * If the entity is a sink, then a tuple with a fields containing <code>null</code> is collected,
 * <code>0</code> otherwise.
 */
public class SinkAggregateBy
extends AggregateBy {

  private static final long serialVersionUID = 4638388000701875436L;

  /**
   * Create a new {@link SinkAggregateBy} instance
   * @param argFields the argument {@link Fields}
   * @param decFields the declared {@link Fields}
   */
  public SinkAggregateBy(Fields argFields, Fields decFields) {
    super(argFields, new SinkAggregatePartials(decFields), new SinkAggregate(decFields));
  }

}
