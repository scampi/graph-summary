/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.entity;

import static org.sindice.core.analytics.util.AnalyticsCounters.TIME;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import java.nio.charset.Charset;

import org.apache.hadoop.io.BytesWritable;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.sindice.analytics.entity.AnalyticsValue;
import org.sindice.analytics.entity.BlankNode;
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
 * This {@link Function} reverse the edges of an entity.
 * <p>
 * The first field serves as a flag, equal to <code>0</code> if the edges are the outgoing ones;
 * else it is equal to <code>1</code> if the edge was reversed. Also, we append the suffix <code>-1</code> to
 * a reversed edge label.
 * <p>
 * The label of the incoming edge is dropped.
 */
public class InverseEdgesDropPredicate
extends SummaryBaseOperation<Tuple>
implements Function<Tuple> {

  private static final long   serialVersionUID = 3273791578113777537L;

  /** This serves as a substitute for the incoming edge label */
  private final static byte[] DUMMY_PREDICATE  = ":".getBytes(Charset.forName("UTF-8"));

  /** Flag to indicate a reversed edge */
  public static final int     IN_FLAG          = 1;
  /** Flag to indicate an outgoing edge */
  public static final int     OUT_FLAG         = 0;

  public InverseEdgesDropPredicate(final Fields fields) {
    super(5, fields);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Tuple> operationCall) {
    super.prepare(flowProcess, operationCall);
    operationCall.setContext(Tuple.size(6));
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Tuple> functionCall) {
    final long start = System.currentTimeMillis();

    final TupleEntry args = functionCall.getArguments();
    final Tuple tuple = functionCall.getContext();

    final AnalyticsValue s = (AnalyticsValue) args.getObject("s");
    final AnalyticsValue p = (AnalyticsValue) args.getObject("p");
    final AnalyticsValue o = (AnalyticsValue) args.getObject("o");

    // set the domain
    tuple.set(1, args.getObject("domain"));

    // Outgoing edge
    setTuple(tuple, OUT_FLAG, args.getObject("subject-hash"), s, p, o);
    functionCall.getOutputCollector().add(tuple);

    // Incoming edge
    if (s instanceof URI && !(o instanceof Literal) && !AnalyticsClassAttributes.isClass(p)) {
      p.setValue(DUMMY_PREDICATE);
      final BytesWritable objHash = BlankNode.getHash128(o);
      setTuple(tuple, IN_FLAG, objHash, o, p, s);
      functionCall.getOutputCollector().add(tuple);
    }

    flowProcess.increment(JOB_ID, TIME + this.getClass().getSimpleName(), System.currentTimeMillis() - start);
  }

  /**
   * Set the given data into the {@link Tuple}.
   * @param tuple the {@link Tuple} to put the data in
   * @param flag whether this edge is incoming or outgoing
   * @param eHash the hash of this entity
   * @param s the subject of the triple
   * @param p the predicate of the triple
   * @param o the object of the triple
   * @return the given {@link Tuple}
   */
  private Tuple setTuple(final Tuple tuple,
                         final int flag,
                         final Object eHash,
                         final AnalyticsValue s,
                         final AnalyticsValue p,
                         final AnalyticsValue o) {
    tuple.set(0, flag);
    tuple.set(2, eHash);
    tuple.set(3, s);
    tuple.set(4, p);
    tuple.set(5, o);
    return tuple;
  }

}
