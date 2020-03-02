/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster.fbisimulation.adjacency;

import static org.sindice.core.analytics.util.AnalyticsCounters.TIME;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import java.util.HashSet;
import java.util.Set;

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
 * The {@link AdjacencyListFunction} class computes the initial cluster identifier of an entity.
 * It is composed of the set of classes and attributes attached to the entity.
 * <p>
 * This function outputs a tuple per outgoing entity into the third fields. It is <code>null</code> if there is no
 * outgoing entity.
 */
public class AdjacencyListFunction
extends SummaryBaseOperation<AdjacencyListFunction.Context>
implements Function<AdjacencyListFunction.Context> {

  private static final long serialVersionUID = -5009623702046179583L;

  class Context {
    final Tuple              tuple = new Tuple();
    final Set<BytesWritable> out   = new HashSet<BytesWritable>();
  }

  public AdjacencyListFunction(final Fields fields) {
    super(fields);
  }

  @Override
  public void prepare(final FlowProcess flowProcess, final OperationCall<Context> operationCall) {
    super.prepare(flowProcess, operationCall);
    operationCall.setContext(new Context());
  }

  @Override
  public void operate(final FlowProcess flowProcess, final FunctionCall<Context> functionCall) {
    final long start = System.currentTimeMillis();

    final TupleEntry args = functionCall.getArguments();
    final Context c = functionCall.getContext();

    final BytesWritable entity = (BytesWritable) args.getObject("subject-hash");
    final EntityDescription eout = (EntityDescription) args.getObject("spo-out");

    eout.setFlowProcess(flowProcess);
    c.out.clear();
    final Statements spos = eout.iterateStatements();
    while (spos.getNext()) {
      if (!AnalyticsClassAttributes.isClass(spos.getPredicate())) {
        // set the incoming entity list
        if (!(spos.getObject() instanceof Literal)) {
          final BytesWritable child = spos.getObjectHash128();
          // no loop
          if (child.equals(entity)) {
            flowProcess.increment(JOB_ID, "Loops", 1);
            continue;
          }
          c.out.add(child);
        }
      }
    }

    final Object cid = args.getObject("cluster-id");
    if (c.out.isEmpty()) {
      c.tuple.clear();
      c.tuple.addAll(args.getLong("domain"), entity, null, cid);
      functionCall.getOutputCollector().add(c.tuple);
    } else {
      for (final BytesWritable o : c.out) {
        c.tuple.clear();
        c.tuple.addAll(args.getLong("domain"), entity, o, cid);
        functionCall.getOutputCollector().add(c.tuple);
      }
    }

    flowProcess.increment(JOB_ID, TIME + this.getClass().getSimpleName(), System.currentTimeMillis() - start);
  }

}
