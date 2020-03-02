package org.sindice.core.analytics.linkanalysis.ding;

import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class LfIdfAggregator
extends BaseOperation<LfIdfAggregator.Context>
implements Aggregator<LfIdfAggregator.Context> {

  private static final long   serialVersionUID = -1817110469636385184L;

  private static final Logger logger           = LoggerFactory
                                               .getLogger(LfIdfAggregator.class);

  public static final String  NUM_VERTICES     = "num-vertices";

  class Context {
    final Tuple tuple = new Tuple();
    final RandomAccessSparseVector sum;
    final int numVertices;
    final double teleProb;

    int startIndex = -1;

    public Context(long numVertices,
                   double teleProb) {
      this.numVertices = (int) numVertices;
      sum = new RandomAccessSparseVector(this.numVertices);
      this.teleProb = teleProb;
    }
  }

  public LfIdfAggregator(Fields fields) {
    super(4, fields);
  }

  @Override
  public void prepare(FlowProcess flowProcess,
                      OperationCall<Context> operationCall) {
    operationCall.setContext(new Context(
      Long.valueOf(flowProcess.getStringProperty(NUM_VERTICES)),
      Double.valueOf(flowProcess.getStringProperty(DingProcess.TELEPORTATION_PROBA))
    ));
  }

  @Override
  public void start(FlowProcess flowProcess,
                    AggregatorCall<Context> aggregatorCall) {
    final Context c = aggregatorCall.getContext();

    c.startIndex = -1;
    c.sum.assign(0);
  }

  @Override
  public void aggregate(FlowProcess flowProcess,
                        AggregatorCall<Context> aggregatorCall) {
    final Context c = aggregatorCall.getContext();
    final TupleEntry args = aggregatorCall.getArguments();

    final int sIndex = args.getInteger("s-index");
    final int oIndex = args.getInteger("o-index");
    final long pDist = args.getLong("p-dist");
    final long weight = args.getLong("total");

    c.startIndex = sIndex;
    if (pDist > c.numVertices) {
      logger.error("the predicate frequency is too big: N=" + c.numVertices + ", but the freq=" + pDist);
      return;
    }
    final double idf = Math.log(c.numVertices / (double) (1 + pDist));
    c.sum.set(oIndex, c.sum.get(oIndex) + weight * idf);
  }

  @Override
  public void complete(FlowProcess flowProcess,
                       AggregatorCall<Context> aggregatorCall) {
    final Context c = aggregatorCall.getContext();
    final Vector v = c.sum.normalize(1).times(c.teleProb);

    if (c.startIndex == -1 || c.sum.maxValue() == 0) {
      logger.error("source-vertice={} weight-vector={}", c.startIndex, c.sum.asFormatString());
      return;
    }
    c.tuple.clear();
    c.tuple.addAll(new IntWritable(c.startIndex), new VectorWritable(v));
    aggregatorCall.getOutputCollector().add(c.tuple);
  }

}
