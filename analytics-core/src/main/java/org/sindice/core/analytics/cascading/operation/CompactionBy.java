/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.operation;

import java.util.Set;

import cascading.pipe.assembly.AggregateBy;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * This {@link AggregateBy} takes the {@link Set} of the argument elements and stores them into a nested {@link Tuple}.
 */
public class CompactionBy extends AggregateBy {

  private static final long serialVersionUID = 4638388000701875436L;

  /**
   * Create A {@link CompactionBy} instance with the given declared {@link Fields}.
   */
  public CompactionBy(Fields decFields) {
    super(decFields, new CompactionPartials(decFields), new CompactionAggregator(decFields));
  }

  /**
   * Create A {@link CompactionBy} instance with the given argument and declared {@link Fields}.
   */
  public CompactionBy(Fields argFields, Fields decFields) {
    super(argFields, new CompactionPartials(decFields), new CompactionAggregator(decFields));
  }

}
