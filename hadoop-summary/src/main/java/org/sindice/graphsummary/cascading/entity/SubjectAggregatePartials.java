/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.entity;

import static org.sindice.core.analytics.util.AnalyticsCounters.TIME;
import static org.sindice.graphsummary.cascading.InstanceCounters.INSTANCE_ID;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import org.sindice.analytics.entity.AnalyticsValue;
import org.sindice.analytics.entity.EntityDescription;
import org.sindice.analytics.entity.EntityDescriptionFactory;
import org.sindice.core.analytics.util.AnalyticsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.pipe.assembly.AggregateBy.Functor;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * This {@link Functor} aggregates the edges describing an entity, map-side.
 * {@link Tuple}s which field <b>flag</b> is not equal to the given <b>inOut</b> value are filtered out.
 * If <b>inOut</b> is <code>null</code>, then no {@link Tuple} is filtered.
 * <p>
 * If either field named "s", "p", or "o" are <code>null</code>, the tuple is skipped.
 */
public class SubjectAggregatePartials implements Functor {

  private static final Logger logger           = LoggerFactory.getLogger(SubjectAggregatePartials.class);

  private static final long   serialVersionUID = -6999276701559286491L;
  private final Fields        declaredFields;
  /** The flag as described in {@link SubjectAggregateBy#SubjectAggregateBy(Fields, Fields, Integer)} */
  private final Integer       inOut;
  /** Threshold on the number of triples for logging large entities */
  private final static int    LARGE            = 50000;

  public SubjectAggregatePartials(Fields declaredFields, Integer inOut) {
    this.declaredFields = declaredFields;
    this.inOut = inOut;

    if (declaredFields.size() != 1) {
      throw new IllegalArgumentException("declared fields can only have 1 fields, got: " + declaredFields);
    }
  }

  @Override
  public Tuple aggregate(FlowProcess flowProcess, TupleEntry args, Tuple context) {
    final long start = System.currentTimeMillis();

    if (context == null) {
      context = new Tuple(EntityDescriptionFactory.getEntityDescription(flowProcess));
    }

    final boolean bool;
    if (inOut != null && args.getInteger("flag") != inOut) {
      bool = false;
    } else {
      bool = true;
    }

    final AnalyticsValue subject = (AnalyticsValue) args.getObject("s");
    final AnalyticsValue predicate = (AnalyticsValue) args.getObject("p");
    final AnalyticsValue object = (AnalyticsValue) args.getObject("o");

    if (bool && subject != null && predicate != null && object != null) {
      ((EntityDescription) context.getObject(0)).add(subject, predicate, object);
    }

    flowProcess.increment(JOB_ID, TIME + this.getClass().getSimpleName(), System.currentTimeMillis() - start);
    return context;
  }

  @Override
  public Tuple complete(FlowProcess flowProcess, Tuple context) {
    final EntityDescription set = (EntityDescription) context.getObject(0);

    AnalyticsUtil.incrementByRange(flowProcess, INSTANCE_ID, getClass().getSimpleName() + " - nb entities",
      set.getNbStatements(), 10, 100, 500, 1000, 5000, 10000, 50000);
    if (set.getNbStatements() > LARGE) {
      logger.error("Entity of size {}: [{}]", set.getNbStatements(), set.getEntity());
    }
    return context;
  }

  @Override
  public Fields getDeclaredFields() {
    return declaredFields;
  }

}
