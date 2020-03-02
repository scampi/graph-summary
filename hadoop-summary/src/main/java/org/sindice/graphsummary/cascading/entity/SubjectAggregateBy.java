/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.entity;

import cascading.pipe.assembly.AggregateBy;
import cascading.tuple.Fields;

/**
 * This {@link AggregateBy} aggregates the edges describing an entity.
 */
public class SubjectAggregateBy extends AggregateBy {

  private static final long serialVersionUID = 4638388000701875436L;

  /**
   * Create an instance with the given argument an declaration {@link Fields}.
   * @param argFields the argument {@link Fields}
   * @param decField the declared {@link Fields}
   */
  public SubjectAggregateBy(Fields argFields, Fields decField) {
    this(argFields, decField, null);
  }

  /**
   * Create an instance with the given argument an declaration {@link Fields}.
   * Tuples which field <b>flag</b> is equal to inOut are kept, the others are filtered out.
   * @param argFields the argument {@link Fields}
   * @param decField the declared {@link Fields}
   * @param inOut the value of the flag
   */
  public SubjectAggregateBy(Fields argFields, Fields decField, final Integer inOut) {
    super(argFields, new SubjectAggregatePartials(decField, inOut), new SubjectAggregate(decField));
  }

}
