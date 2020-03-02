/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.entity;

import static org.sindice.core.analytics.util.AnalyticsCounters.TIME;
import static org.sindice.graphsummary.cascading.InstanceCounters.CLASS_BNODE;
import static org.sindice.graphsummary.cascading.InstanceCounters.CLASS_EMPTY;
import static org.sindice.graphsummary.cascading.InstanceCounters.INSTANCE_ID;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.sindice.analytics.entity.AnalyticsValue;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
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
 * Clean the classes of an entity.
 * 
 * <p>
 * 
 * An object is a class if the predicate is a class attribute, i.e.,
 * for which the method {@link AnalyticsClassAttributes#isClass(AnalyticsValue)}
 * is <code>true</code>.
 * 
 * <ul>
 * <li>Filter classes which are {@link BNode};</li>
 * <li>Filter empty classes;</li>
 * <li>If the class is a {@link Literal} and that
 * {@link AnalyticsParameters#NORM_LITERAL_TYPE} is <code>true</code>,
 * the class is normalised (see {@link AnalyticsClassAttributes#normalizeLiteralTypeLabel(String)}).
 * </li>
 * </ul>
 */
public class FilterClasses
extends SummaryBaseOperation<Tuple>
implements Function<Tuple> {

  private static final long serialVersionUID = 7822070483069388098L;

  public FilterClasses(final Fields fields) {
    super(3, fields);
  }

  @Override
  public void prepare(FlowProcess flowProcess,
                      OperationCall<Tuple> operationCall) {
    super.prepare(flowProcess, operationCall);
    operationCall.setContext(Tuple.size(3));
  }

  @Override
  public void operate(FlowProcess flowProcess,
                      FunctionCall<Tuple> fc) {
    final long start = System.currentTimeMillis();

    final TupleEntry args = fc.getArguments();
    final Tuple tuple = fc.getContext();

    final AnalyticsValue predicate = (AnalyticsValue) args.getObject(1);
    final AnalyticsValue object = (AnalyticsValue) args.getObject(2);

    tuple.setAll(args.getTuple());
    if (AnalyticsClassAttributes.isClass(predicate)) {
      if (object instanceof BNode) {
        // I don't want blank nodes for type objects
        flowProcess.increment(INSTANCE_ID, CLASS_BNODE, 1);
        return;
      }
      if (object.getValue().length == 0) {
        // discard this statement, the type has no meaning since it is empty!
        flowProcess.increment(INSTANCE_ID, CLASS_EMPTY, 1);
        return;
      }
      // If type is a literal, normalize only if AnalyticsParameters.NORM_LITERAL_TYPE is true
      if (AnalyticsParameters.NORM_LITERAL_TYPE.get() && object instanceof Literal) {
        final String o = AnalyticsClassAttributes.normalizeLiteralTypeLabel(object.stringValue());
        if (o == null) {
          flowProcess.increment(INSTANCE_ID, CLASS_EMPTY, 1);
          return;
        }
        object.setValue(o);
        tuple.set(2, object);
      }
    }

    fc.getOutputCollector().add(tuple);

    flowProcess.increment(JOB_ID, TIME + this.getClass().getSimpleName(), System.currentTimeMillis() - start);
  }

}
