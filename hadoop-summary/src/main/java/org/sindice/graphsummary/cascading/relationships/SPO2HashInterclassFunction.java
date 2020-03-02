/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.relationships;

import static org.sindice.core.analytics.util.AnalyticsCounters.TIME;
import static org.sindice.graphsummary.cascading.InstanceCounters.INSTANCE_ID;
import static org.sindice.graphsummary.cascading.InstanceCounters.RELATIONS;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import org.apache.hadoop.io.BytesWritable;
import org.openrdf.model.Literal;
import org.sindice.analytics.entity.EntityDescription;
import org.sindice.analytics.entity.EntityDescription.Statements;
import org.sindice.core.analytics.rdf.AnalyticsClassAttributes;
import org.sindice.graphsummary.cascading.SummaryBaseOperation;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Parse function to get fields codified in json
 * 
 * @author Diego Ceccarelli <diego.ceccarelli@deri.org>
 * @link www.di.unipi.it/~ceccarel
 */
public class SPO2HashInterclassFunction
extends SummaryBaseOperation<Tuple>
implements Function<Tuple> {

  private static final long  serialVersionUID  = 844468200762403527L;

  public SPO2HashInterclassFunction(Fields declarationFields) {
    super(4, declarationFields);
  }

  @Override
  public void prepare(final FlowProcess flowProcess,
                      final OperationCall<Tuple> operationCall) {
    super.prepare(flowProcess, operationCall);
    operationCall.setContext(Tuple.size(4));
  }

  @Override
  public void operate(final FlowProcess flowProcess,
                      final FunctionCall<Tuple> functionCall) {
    final long start = System.currentTimeMillis();

    final Tuple tuple = functionCall.getContext();
    final TupleEntry args = functionCall.getArguments();

    final EntityDescription eout = (EntityDescription) args.getObject("spo-out");
    eout.setFlowProcess(flowProcess);
    final long sndDomainHash = args.getLong("domain");
    final BytesWritable subjectHash = (BytesWritable) args.getObject("subject-hash");
    final Statements spos = eout.iterateStatements();

    tuple.set(0, sndDomainHash);
    tuple.set(1, subjectHash);
    while (spos.getNext()) {
      // Check if predicate is defines a class
      if (AnalyticsClassAttributes.isClass(spos.getPredicate())) {
        continue;
      }

      /*
       * If the predicate does not define a class, output a relationship for
       * each object
       */
      if (!(spos.getObject() instanceof Literal)) {

        flowProcess.increment(INSTANCE_ID, RELATIONS, 1);

        final BytesWritable objectHash = spos.getObjectHash128();
        final long predicateHash = spos.getPredicateHash64();
        tuple.set(2, predicateHash);
        tuple.set(3, objectHash);
        functionCall.getOutputCollector().add(tuple);
      }
    }

    flowProcess.increment(JOB_ID, TIME + this.getClass().getSimpleName(),
      System.currentTimeMillis() - start);
  }

}
