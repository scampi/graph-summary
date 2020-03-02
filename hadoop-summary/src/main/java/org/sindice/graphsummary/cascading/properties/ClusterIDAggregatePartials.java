/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.properties;

import static org.sindice.core.analytics.util.AnalyticsCounters.TIME;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;
import cascading.flow.FlowProcess;
import cascading.pipe.assembly.AggregateBy.Functor;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.Tuples;

/**
 * {@link Functor} of the {@link ClusterIDAggregateBy}.
 */
public class ClusterIDAggregatePartials implements Functor {

  private static final long serialVersionUID = 6646014515306127789L;
  private final Fields      declaredFields;

  public ClusterIDAggregatePartials(Fields declaredFields) {
    this.declaredFields = declaredFields;

    if (declaredFields.size() != 2) {
      throw new IllegalArgumentException("declared fields can only have two fields, got: " + declaredFields);
    }
  }

  @Override
  public Tuple aggregate(final FlowProcess flowProcess, final TupleEntry args, Tuple context) {
    final long start = System.currentTimeMillis();

    if (context == null) {
      // two fields: attributes and types
      context = new Tuple(new TypesCount(), new PropertiesCount());
    }

    final int flag = args.getInteger("flag");
    final long label = args.getLong("label");
    final Object value = args.getObject("value");
    // Attributes
    if (flag == GetPropertiesGraph.ATTRIBUTE) {
      final PropertiesCount attributes = (PropertiesCount) context.getObject(1);
      if (value == null) { // no datatype
        attributes.add(label, PropertiesCount.NO_DATATYPE);
      } else {
        attributes.add(label, Tuples.toLong(value));
      }
    } else if (flag == GetPropertiesGraph.TYPE) { // Types
      final TypesCount types = (TypesCount) context.getObject(0);
      types.add(label, ((Integer) value).byteValue());
    }

    flowProcess.increment(JOB_ID, TIME + this.getClass().getSimpleName(), System.currentTimeMillis() - start);
    return context;
  }

  @Override
  public Tuple complete(FlowProcess flowProcess, Tuple context) {
    return context;
  }

  @Override
  public Fields getDeclaredFields() {
    return declaredFields;
  }

}
