/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.operation;

import java.util.Random;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * This {@link Function} inserts a random value between 0 included and a given upper bound, excluded.
 * The upper bound can be explicitly set via the constructor. If not, it will be set to the number of reducers.
 */
public class InsertRandomValue
extends BaseOperation<InsertRandomValue.Context>
implements Function<InsertRandomValue.Context> {

  private static final long serialVersionUID = -3928213780522025982L;

  private final int         uBound;

  class Context {
    final Tuple  tuple = Tuple.size(1);
    final Random rand  = new Random(42);
    final int    uBound;

    public Context(FlowProcess flowProcess) {
      if (InsertRandomValue.this.uBound != -1) {
        uBound = InsertRandomValue.this.uBound;
        return;
      }

      final String nb = flowProcess.getStringProperty("mapred.reduce.tasks");
      if (nb == null) {
        uBound = 1;
      } else {
        uBound = Integer.valueOf(nb);
      }
    }
  }

  public InsertRandomValue(Fields fields) {
    this(fields, -1);
  }

  /**
   * Create a new {@link InsertRandomValue} instance with the given upper bound.
   * @param fields the declared {@link Fields}
   * @param uBound the upper bound
   */
  public InsertRandomValue(Fields fields, int uBound) {
    super(fields);
    this.uBound = uBound;
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall) {
    operationCall.setContext(new Context(flowProcess));
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Context> functionCall) {
    final Context c = functionCall.getContext();

    c.tuple.set(0, c.rand.nextInt(c.uBound));
    functionCall.getOutputCollector().add(c.tuple);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof InsertRandomValue)) {
      return false;
    }
    if (!super.equals(object)) {
      return false;
    }

    final InsertRandomValue rand = (InsertRandomValue) object;
    return rand.uBound == uBound;
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = hash * 31 + uBound;
    return hash;
  }

}
