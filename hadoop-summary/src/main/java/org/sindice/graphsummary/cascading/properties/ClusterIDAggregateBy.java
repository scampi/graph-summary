/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.properties;

import cascading.pipe.assembly.AggregateBy;
import cascading.tuple.Fields;

/**
 * This {@link AggregateBy} aggregates statistics about the properties and types associated with a cluster.
 */
public class ClusterIDAggregateBy extends AggregateBy {

  private static final long serialVersionUID = -6072414608280977871L;

  /**
   * Create a new {@link ClusterIDAggregateBy} instance
   * @param argField the argument {@link Fields}
   * @param decField the declared {@link Fields}
   */
  public ClusterIDAggregateBy(Fields argField, Fields decField) {
    super(argField, new ClusterIDAggregatePartials(decField), new ClusterIDAggregate(decField));
  }

}
