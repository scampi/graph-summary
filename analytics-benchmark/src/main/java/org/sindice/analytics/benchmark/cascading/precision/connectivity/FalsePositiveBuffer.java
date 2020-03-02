/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.precision.connectivity;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.BytesWritable;
import org.sindice.core.analytics.util.AnalyticsException;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.util.Pair;

/**
 * This {@link Buffer} computes the number of false positive edges found between unconnected nodes
 * of the gold standard that do get connected via the evaluated summary.
 * <p>
 * A group is based on the node identifier of the evaluated summary that is at the center of a star graph.
 * Then, this operation computes all the pairs of virtually connected gold standard nodes, and counts the
 * number of created edges. This number updates then the number of false positive edges found in
 * {@link ConnectedTFPositive}.
 * <p>
 * This buffer outputs a tuple with 5 {@link Fields} per src-eval / src-gold pair:
 * <ul>
 * <li><b>domain</b>: the domain of the linkset;</li>
 * <li><b>cid-eval</b>: the identifier of the evaluated cluster;</li>
 * <li><b>cid-gold</b>: the identifier of the gold cluster inside cid-eval;</li>
 * <li><b>true</b>: the number of true positive edges which tail is cid-gold; and</li>
 * <li><b>false</b>: the number of false positive edges which tail is cid-gold.</li>
 * </ul>
 * <p>
 * The <b>cid-gold</b> is <code>null</code> in the case where:
 * <ul>
 * <li>that cluster is an island, i.e., no incoming nor outgoing edges;</li>
 * <li>that cluster is not connected to any gold-cluster of a dst-eval; or</li>
 * <li>as a subset of the previous case, that cluster is not connected to <em>any</em> dst-eval.</li>
 * </ul>
 */
public class FalsePositiveBuffer
extends BaseOperation<FalsePositiveBuffer.Context>
implements Buffer<FalsePositiveBuffer.Context> {

  private static final long   serialVersionUID = -6552359062786615431L;

  class Context {
    final Map<BytesWritable, Pair<Tuple, Long>> connected = new HashMap<BytesWritable, Pair<Tuple, Long>>();
    final Tuple                                 tuple     = new Tuple(null, null, 0L, 0L, 0L);
  }

  public FalsePositiveBuffer(final Fields fields) {
    super(8, fields);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall) {
    operationCall.setContext(new Context());
  }

  @Override
  public void operate(FlowProcess flowProcess, BufferCall<Context> bufferCall) {
    final Context c = bufferCall.getContext();
    final Iterator<TupleEntry> it = bufferCall.getArgumentsIterator();
    final BytesWritable srcEval = (BytesWritable) bufferCall.getGroup().getObject("src-eval");

    BytesWritable prevDstEval = null;
    c.connected.clear();

    BytesWritable srcGold = null;
    BytesWritable dstEval = null;
    long ePreds = 0;
    long srcEvalCount = 0;
    long dstEvalCount = 0;

    // Get the links
    while (it.hasNext()) {
      final TupleEntry te = it.next();

      dstEval = (BytesWritable) te.getObject("dst-eval");

      if (prevDstEval != null && !prevDstEval.equals(dstEval)) { // new dst-eval
        computedFalsePositives(c, srcEval, srcEvalCount, prevDstEval, dstEvalCount, ePreds,
          bufferCall.getOutputCollector());
        c.connected.clear();
      }

      srcGold = (BytesWritable) te.getObject("src-gold");
      ePreds = te.getLong("predicates-eval");
      srcEvalCount = te.getLong("src-eval-count");
      dstEvalCount = te.getLong("dst-eval-count");

      final Pair<Tuple, Long> pair;
      if (!c.connected.containsKey(srcGold)) {
        c.connected.put(srcGold, new Pair<Tuple, Long>(new Tuple(null, null, 0L, 0L, 0L), 0L));
      }
      pair = c.connected.get(srcGold);

      // number of dst-gold this src-gold connects to
      pair.setRhs(pair.getRhs() + 1);
      // update true/false positives
      pair.getLhs().set(3, te.getLong("true") + pair.getLhs().getLong(3));
      pair.getLhs().set(4, te.getLong("false") + pair.getLhs().getLong(4));

      prevDstEval = dstEval;
      // I am alive!
      flowProcess.keepAlive();
    }
    // process last dst-eval
    computedFalsePositives(c, srcEval, srcEvalCount, dstEval, dstEvalCount, ePreds, bufferCall.getOutputCollector());
  }

  /**
   * Computes the number of false positives links from the gold clusters seen so far in src-eval to dst-eval.
   * @param c the {@link Context}
   * @param srcEval the identifier of the evaluated cluster at the tail of the linkset
   * @param srcEvalCount the total number of gold clusters in src-eval
   * @param dstEval the identifier of the evaluated cluster at the head of the linkset
   * @param dstEvalCount the total number of gold clusters in dst-eval
   * @param ePreds the number of predicates in the linkset
   * @param out the {@link TupleEntryCollector}
   */
  private void computedFalsePositives(final Context c,
                                      final BytesWritable srcEval,
                                      final long srcEvalCount,
                                      final BytesWritable dstEval,
                                      final long dstEvalCount,
                                      final long ePreds,
                                      final TupleEntryCollector out) {
    for (Entry<BytesWritable, Pair<Tuple, Long>> src : c.connected.entrySet()) {
      final Tuple tuple = src.getValue().getLhs();
      final long connected = src.getValue().getRhs();

      tuple.set(0, srcEval);
      tuple.set(1, src.getKey());
      tuple.set(2, srcEvalCount);
      // false positive links from this srcGold to the rest of the dst-eval cluster
      if (dstEvalCount < connected) {
        throw new AnalyticsException("src-eval=[" + srcEval + "] dstEval=[" + dstEval + "] srcGold=[" + src.getKey() +
          "] totalSize=" + dstEvalCount + " connected=" + connected);
      }
      final long fp1 = (dstEvalCount - connected) * ePreds;
      tuple.set(4, tuple.getLong(4) + fp1);
      out.add(tuple);
    }

    // false positive links from islands and nodes that don't connect to dst-eval (but does in eval)
    if (srcEvalCount < c.connected.size()) {
      throw new AnalyticsException("src-eval=[" + srcEval + "] dstEval=[" + dstEval + "] srcEvalCount=" +
        dstEvalCount + " connected(size)=" + c.connected.size());
    }
    final long fp2 = (srcEvalCount - c.connected.size()) * dstEvalCount * ePreds;
    if (fp2 != 0) {
      c.tuple.set(0, srcEval);
      c.tuple.set(1, null);
      c.tuple.set(2, srcEvalCount);
      c.tuple.set(3, 0L);
      c.tuple.set(4, fp2);
      out.add(c.tuple);
    }
  }

}
