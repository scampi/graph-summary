/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.entity;

import static org.sindice.core.analytics.util.AnalyticsCounters.TIME;
import static org.sindice.graphsummary.cascading.InstanceCounters.EFFECTIVE_TRIPLES;
import static org.sindice.graphsummary.cascading.InstanceCounters.ENTITIES;
import static org.sindice.graphsummary.cascading.InstanceCounters.INSTANCE_ID;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import org.sindice.analytics.entity.EntityDescription;
import org.sindice.analytics.entity.EntityDescriptionFactory;
import org.sindice.core.analytics.util.AnalyticsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * This class aggregates the edges describing an entity.
 * If there is no such edge, a field with a <code>null</null> value is collected.
 * Otherwise the field is set with the data from an {@link EntityDescription} implementation.
 */
public class SubjectAggregate
extends BaseOperation<SubjectAggregate.Context>
implements Aggregator<SubjectAggregate.Context> {

  private static final Logger logger           = LoggerFactory.getLogger(SubjectAggregate.class);

  private static final long   serialVersionUID = -3900874977420493352L;

  /** Threshold on the number of triples for logging large entities */
  private final static int    LARGE            = 50000;

  class Context {
    final Tuple tuple = Tuple.size(1);
    final EntityDescription eDesc;
    public Context(FlowProcess fp) {
      eDesc = EntityDescriptionFactory.getEntityDescription(fp);
    }
  }

  public SubjectAggregate(Fields fieldDeclaration) {
    super(1, fieldDeclaration);
  }

  @Override
  public void prepare(FlowProcess flowProcess,
                      OperationCall<Context> operationCall) {
    operationCall.setContext(new Context(flowProcess));
  }

  public void start(FlowProcess flowProcess,
                    AggregatorCall<Context> aggregatorCall) {
    aggregatorCall.getContext().eDesc.reset();
  }

  public void aggregate(FlowProcess flowProcess,
                        AggregatorCall<Context> aggregatorCall) {
    final long start = System.currentTimeMillis();

    final Context c = aggregatorCall.getContext();

    final EntityDescription eDesc = (EntityDescription) aggregatorCall.getArguments().getObject(0);

    eDesc.setFlowProcess(flowProcess);
    // aggregate the spo
    c.eDesc.add(eDesc);

    flowProcess.increment(JOB_ID, TIME + this.getClass().getSimpleName(), System.currentTimeMillis() - start);
  }

  public void complete(FlowProcess flowProcess,
                       AggregatorCall<Context> aggregatorCall) {
    final long start = System.currentTimeMillis();

    final Context c = aggregatorCall.getContext();

    // If no statements were aggregated for this entity, then a tuple with a null value is outputed
    c.tuple.set(0, null);
    if (c.eDesc.getNbStatements() != 0) {
      c.tuple.set(0, c.eDesc);

      // logging info
      flowProcess.increment(INSTANCE_ID, EFFECTIVE_TRIPLES, c.eDesc.getNbStatements());
      flowProcess.increment(INSTANCE_ID, ENTITIES, c.eDesc.getNbStatements() == 0 ? 0 : 1);
      AnalyticsUtil.incrementByRange(flowProcess, INSTANCE_ID, getClass().getSimpleName() + " - nb entities",
        c.eDesc.getNbStatements(), 10, 100, 500, 1000, 5000, 10000, LARGE);
      if (c.eDesc.getNbStatements() > LARGE) {
        logger.error("Entity of size {}: [{}]", c.eDesc.getNbStatements(), c.eDesc.getEntity());
      }
    }
    aggregatorCall.getOutputCollector().add(c.tuple);

    flowProcess.increment(JOB_ID, TIME + this.getClass().getSimpleName(), System.currentTimeMillis() - start);
  }

}
