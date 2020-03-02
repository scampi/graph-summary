package org.sindice.core.analytics.linkanalysis.ding;

import java.util.ArrayList;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class Index
extends BaseOperation<Index.Context>
implements Aggregator<Index.Context> {

  private static final long serialVersionUID = 8980832591802390071L;

  class Context {
    final Tuple tuple = new Tuple();
    final ArrayList<String> datasets = new ArrayList<String>();
  }

  public Index(Fields fields) {
    super(1, fields);
  }

  @Override
  public void start(FlowProcess flowProcess,
                    AggregatorCall<Context> aggregatorCall) {
    aggregatorCall.setContext(new Context());
  }

  @Override
  public void aggregate(FlowProcess flowProcess,
                        AggregatorCall<Context> aggregatorCall) {
    final TupleEntry args = aggregatorCall.getArguments();
    final Context c = aggregatorCall.getContext();

    c.datasets.add(args.getString("dataset"));
  }

  @Override
  public void complete(FlowProcess flowProcess,
                       AggregatorCall<Context> aggregatorCall) {
    final Context c = aggregatorCall.getContext();
    int index = 0;

    for (String dataset: c.datasets) {
      c.tuple.clear();
      c.tuple.addAll(dataset, index++);
      aggregatorCall.getOutputCollector().add(c.tuple);
    }
  }

}
