/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.operation;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Tuple;

/**
 * This {@link Filter} removes {@link Tuple}s that don't have at least one of the given values,
 * as per their {@link #equals(Object)} method.
 */
public class DifferentValueFilter
extends BaseOperation<Void>
implements Filter<Void> {

  private static final long serialVersionUID = -3021940472129485308L;
  private final Object[]    toKeep;

  public DifferentValueFilter(Object... toKeep) {
    super(1);
    if (toKeep == null || toKeep.length == 0) {
      throw new IllegalArgumentException("Missing values that are to be kept");
    }
    this.toKeep = toKeep;
  }

  public boolean isRemove(FlowProcess flowProcess, FilterCall<Void> filterCall) {
    return !hasKey(filterCall.getArguments().getObject(0));
  }

  private boolean hasKey(Object s) {
    for (Object tk : toKeep) {
      if (tk.equals(s)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof DifferentValueFilter)) {
      return false;
    }
    if (!super.equals(object)) {
      return false;
    }

    final DifferentValueFilter diff = (DifferentValueFilter) object;
    for (int i = 0; i < toKeep.length; i++) {
      if ((toKeep[i] == null && diff.toKeep[i] != null) || !(toKeep[i].equals(diff.toKeep[i]))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    for (Object o : toKeep) {
      hash = hash * 31 + (o == null ? 0 : o.hashCode());
    }
    return hash;
  }

}
