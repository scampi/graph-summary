/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.operation;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.TupleEntry;

/**
 * This {@link Filter} removes tuple if the value is less than or equals to some value
 */
public class LessThanOrEqualsFilter
extends BaseOperation<Void>
implements Filter<Void> {

  private static final long serialVersionUID = 1L;
  private final String field;
  private final Class<? extends Number> type;
  private final int threshold;

  public LessThanOrEqualsFilter(String field,
                                int threshold,
                                Class<? extends Number> type) {
    super();
    this.field = field;
    this.type = type;
    this.threshold = threshold;
  }

  public boolean isRemove(FlowProcess flowProcess, FilterCall<Void> filterCall) {
    final TupleEntry arguments = filterCall.getArguments();
    Number n = null;

    if (type == Integer.class)
      n = arguments.getInteger(field);
    else if (type == Long.class)
      n = arguments.getLong(field);
    else if (type == float.class)
      n = arguments.getFloat(field);
    else if (type == Double.class)
      n = arguments.getDouble(field);
    else
      throw new IllegalArgumentException("Not a Valid Class");

    return (n.longValue() <= threshold);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof LessThanOrEqualsFilter)) {
      return false;
    }
    if (!super.equals(object)) {
      return false;
    }

    final LessThanOrEqualsFilter filter = (LessThanOrEqualsFilter) object;
    if (!(field.equals(filter.field))) {
      return false;
    }
    if (!(type.equals(filter.type))) {
      return false;
    }
    return threshold == filter.threshold;
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = hash * 31 + field.hashCode();
    hash = hash * 31 + type.hashCode();
    hash = hash * 31 + threshold;
    return hash;
  }

}
