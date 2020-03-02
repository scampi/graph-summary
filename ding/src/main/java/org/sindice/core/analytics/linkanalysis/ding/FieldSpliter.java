package org.sindice.core.analytics.linkanalysis.ding;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class FieldSpliter
extends BaseOperation<FieldSpliter.Context>
implements Function<FieldSpliter.Context> {

  private static final long serialVersionUID = -5239120149552889L;

  class Context {
    final Tuple tuple = new Tuple();
  }

  public FieldSpliter(Fields fields) {
    super(fields);
  }

  @Override
  public void prepare(FlowProcess flowProcess,
                      OperationCall<Context> operationCall) {
    operationCall.setContext(new Context());
  }

  @Override
  public void operate(FlowProcess flowProcess,
                      FunctionCall<Context> functionCall) {
    final Context c = functionCall.getContext();
    final TupleEntry args = functionCall.getArguments();

    for (int i = 0; i < args.size(); i++) {
      final Object o = args.getObject(i);
      c.tuple.clear();
      c.tuple.add(o);
      functionCall.getOutputCollector().add(c.tuple);
    }
  }

}
