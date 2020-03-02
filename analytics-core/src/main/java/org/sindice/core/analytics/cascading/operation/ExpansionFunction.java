/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.operation;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * This function expands a nested {@link Tuple}, creating a {@link Tuple} for each of its element.
 * <p>
 * The {@link Tuple} is in position <code>0</code> of the argument tuple by default. Another position may
 * be given via the constructor. If it is <code>null</code>, no tuple is collected. All remaining values of
 * the argument are passed to the collected tuples.
 */
public class ExpansionFunction
extends BaseOperation<Tuple>
implements Function<Tuple> {

  private static final long serialVersionUID = 2100689378883052887L;

  private final int pos;

  public ExpansionFunction(Fields fields) {
    this(fields, 0);
  }

  /**
   * Create a new instance with fields as declared {@link Fields}, and where
   * the position of the {@link Tuple} is at pos.
   * @param fields of type {@link Fields}
   * @param pos the position of the {@link Tuple} in the arguments
   */
  public ExpansionFunction(Fields fields, int pos) {
    super(fields);
    this.pos = pos;
  }

  @Override
  public void prepare(final FlowProcess flowProcess,
                      final OperationCall<Tuple> operationCall) {
    operationCall.setContext(Tuple.size(getFieldDeclaration().size()));
  }

  @Override
  public void operate(final FlowProcess flowProcess,
                      final FunctionCall<Tuple> functionCall) {
    final TupleEntry args = functionCall.getArguments();
    final Tuple tuple = functionCall.getContext();
    final Tuple bs = (Tuple) args.getObject(pos);

    if (bs == null) {
      return;
    }
    tuple.setAll(args.getTuple());
    for (Object o : bs) {
      tuple.set(pos, o);
      functionCall.getOutputCollector().add(tuple);
    }
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof ExpansionFunction)) {
      return false;
    }
    if (!super.equals(object)) {
      return false;
    }

    final ExpansionFunction flatten = (ExpansionFunction) object;
    return flatten.pos == pos;
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = hash * 31 + pos;
    return hash;
  }

}
