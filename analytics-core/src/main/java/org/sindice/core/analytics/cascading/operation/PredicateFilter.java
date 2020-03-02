/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.operation;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * This {@link Filter} removes {@link Tuple}s which {@link String} field value is not in the specified predicates.
 */
public class PredicateFilter
extends BaseOperation<Void>
implements Filter<Void> {

  private static final long serialVersionUID = 1L;

  private static final Logger logger = LoggerFactory
      .getLogger(PredicateFilter.class);

  final Set<String> filters = new HashSet<String>();

  public PredicateFilter(String predicate) {
    super();
    this.filters.add(predicate);
  }

  public PredicateFilter(Set<String> filters) {
    super();
    this.filters.addAll(filters);
  }

  public boolean isRemove(FlowProcess flowProcess, FilterCall<Void> filterCall) {
    boolean isRemove = false;
    final TupleEntry arguments = filterCall.getArguments();
    final String predicate = arguments.getString(0);

    for (String s : filters) {
      if (predicate.contains(s)) {
        isRemove = true;
        logger.debug("remove predicate {} matching with rule {} = -",
            predicate, s);
        break;
      }
    }
    /* if the predicate is in the list, it must be removed */
    return isRemove;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof PredicateFilter)) {
      return false;
    }
    if (!super.equals(object)) {
      return false;
    }

    final PredicateFilter func = (PredicateFilter) object;
    return filters.equals(func.filters);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = hash * 31 + filters.hashCode();
    return hash;
  }

}
