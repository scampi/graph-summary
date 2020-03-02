/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.operation;

import java.util.HashSet;
import java.util.Set;

import org.sindice.core.analytics.util.AnalyticsCounters;
import org.sindice.core.analytics.util.AnalyticsUtil;

import cascading.flow.FlowProcess;
import cascading.pipe.assembly.AggregateBy.Functor;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * This {@link Functor} keeps a set of the tuple elements and stores them into a {@link Tuple}.
 */
public class CompactionPartials
implements Functor {

  private static final long serialVersionUID = -1072588431211286431L;
  private final Fields      declaredFields;

  public CompactionPartials(Fields declaredFields) {
    this.declaredFields = declaredFields;

    if (declaredFields.size() != 1) {
      throw new IllegalArgumentException("declared fields can only have 1 fields, got: " + declaredFields);
    }
  }

  @Override
  public Tuple aggregate(final FlowProcess flowProcess, final TupleEntry args, Tuple context) {
    if (context == null) {
      context = new Tuple(new HashSet<Comparable>());
    }

    final Comparable o = (Comparable) args.getObject(0);
    if (o != null) {
      getElements((Set<Comparable>) context.getObject(0), o);
    }
    return context;
  }

  /**
   * Puts the {@link Comparable} o into the given {@link Set}. If o is a {@link Tuple}, puts
   * all its elements into the set instead.
   */
  private void getElements(final Set<Comparable> set, final Comparable o) {
    if (o instanceof Tuple) {
      final Tuple tuple = (Tuple) o;
      for (int i = 0; i < tuple.size(); i++) {
        getElements(set, (Comparable) tuple.getObject(i));
      }
    } else {
      set.add(o);
    }
  }

  @Override
  public Tuple complete(final FlowProcess flowProcess, final Tuple context) {
    final Set<Comparable> objs = (Set<Comparable>) context.getObject(0);

    AnalyticsUtil.incrementByRange(flowProcess, AnalyticsCounters.GROUP, CompactionBy.class.getSimpleName() + "_set",
      objs.size(), 10, 50, 100, 500, 1000, 5000, 10000, 20000, 50000, 100000);
    final Tuple compact = Tuple.size(objs.size());
    int cnt = 0;
    for (Comparable c : objs) {
      compact.set(cnt++, c);
    }
    final Tuple t = Tuple.size(1);
    t.set(0, compact);
    return t;
  }

  @Override
  public Fields getDeclaredFields() {
    return declaredFields;
  }

}
