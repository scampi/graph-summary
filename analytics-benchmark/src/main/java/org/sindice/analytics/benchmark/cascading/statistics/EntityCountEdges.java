/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.statistics;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.BytesWritable;
import org.openrdf.model.Literal;
import org.sindice.analytics.entity.EntityDescription;
import org.sindice.analytics.entity.EntityDescription.Statements;
import org.sindice.core.analytics.util.Hash;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * This {@link Function} counts the number of edges in the data graph.
 * It outputs two fields: in first position is the {@link Hash#getHash128(Iterable) hash} of the object
 * identifier, and in second position is the count of edges that connect to it. If the object is a
 * {@link Literal}, then the first position is equal to <code>null</code>.
 */
public class EntityCountEdges
extends BaseOperation<EntityCountEdges.Context>
implements Function<EntityCountEdges.Context> {

  private static final long serialVersionUID = 8198382867380655471L;

  class Context {
    final Tuple                    tuple   = Tuple.size(2);
    final Map<BytesWritable, Long> degrees = new HashMap<BytesWritable, Long>();
  }

  public EntityCountEdges(final Fields fields) {
    super(fields);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall) {
    operationCall.setContext(new Context());
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Context> functionCall) {
    final TupleEntry args = functionCall.getArguments();
    final Context c = functionCall.getContext();
    final EntityDescription eout = (EntityDescription) args.getObject("spo-out");

    c.degrees.clear();
    eout.setFlowProcess(flowProcess);
    final Statements it = eout.iterateStatements();
    long degLit = 0;
    while (it.getNext()) {
      if (it.getObject() instanceof Literal) {
        degLit++;
      } else {
        final BytesWritable o = it.getObjectHash128();
        if (!c.degrees.containsKey(o)) {
          c.degrees.put(o, 0l);
        }
        c.degrees.put(o, c.degrees.get(o) + 1);
      }
    }

    // the count of literals
    if (degLit != 0) {
      c.tuple.set(0, null);
      c.tuple.set(1, degLit);
      functionCall.getOutputCollector().add(c.tuple);
    }
    // the count of edges attached to other entities
    for (final Entry<BytesWritable, Long> out : c.degrees.entrySet()) {
      c.tuple.set(0, out.getKey());
      c.tuple.set(1, out.getValue());
      functionCall.getOutputCollector().add(c.tuple);
    }
  }

}
