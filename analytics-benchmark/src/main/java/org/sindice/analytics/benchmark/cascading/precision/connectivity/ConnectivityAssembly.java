/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.precision.connectivity;

import org.sindice.analytics.benchmark.cascading.precision.BlankSwitch;
import org.sindice.analytics.benchmark.cascading.precision.Linksets;
import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.sindice.core.analytics.cascading.operation.CompactionBy;

import cascading.operation.Identity;
import cascading.operation.filter.Not;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.pipe.joiner.LeftJoin;
import cascading.tuple.Fields;

/**
 * This {@link AnalyticsSubAssembly} computes the false and true positive edges, using the <b>eval-linkset</b>
 * tail of the {@link Linksets} assembly.
 * <p>
 * This is done via a three-part computation. First, we compute the linksets of blank nodes. Then, we compute the count
 * of the false positive edges between nodes of the gold standard that actually have a relation. Then, the count of
 * false positive edges between nodes that are <b>not</b> connected in the gold standard, but are in the evaluated
 * summary.
 * <p>
 * This assembly has one tail, with the following fields:
 * <ul>
 * <li><b>domain</b>: the domain of the node;</li>
 * <li><b>cid-eval</b>: the node identifier in the evaluated summary;</li>
 * <li><b>cid-gold</b>: the node identifier in the gold standard summary;</li>
 * <li><b>size-eval</b>: the number of nodes from the gold standard included in this evaluated node;</li>
 * <li><b>true</b>: the number of true positive edges; and</li>
 * <li><b>false</b>: the number of false positive edges.</li>
 * </ul>
 */
@AnalyticsName(value="connectivity-assembly")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(sinkName="eval-linkset", from=Linksets.class)
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "domain", "cid-eval", "cid-gold", "size-eval", "true", "false" })
})
public class ConnectivityAssembly
extends AnalyticsSubAssembly {

  private static final long serialVersionUID = -3168434171677345115L;

  public ConnectivityAssembly() {
    super();
  }

  public ConnectivityAssembly(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    /*
     * Replace the predicates-eval associated to a src-eval blank
     */
    Pipe blankSrc = new Pipe("blank-src", previous[0]);
    blankSrc = new Each(blankSrc, new Fields("src-eval", "dst-eval", "predicates-eval"), new Not(new BlankFilter(true)));
    blankSrc = new AggregateBy(blankSrc, new Fields("domain", "src-eval", "dst-eval"),
      new CompactionBy(new Fields("predicates-gold"), new Fields("predicates-eval")));
    final Fields decSrc = new Fields(
      "domain", "src-gold", "src-count-gold", "dst-gold", "dst-count-gold", "predicates-gold",
      "src-eval", "src-eval-count", "dst-eval", "dst-eval-count", "p_eval_l",
      "domain-r", "src-eval-r", "dst-eval-r", "p_eval_r"
    );
    previous[0] = new CoGroup("blank-preds", previous[0], new Fields("domain", "src-eval", "dst-eval"),
      blankSrc, new Fields("domain", "src-eval", "dst-eval"), decSrc, new LeftJoin());
    previous[0] = new Each(previous[0], new Fields("p_eval_l", "p_eval_r"),
      new BlankSwitch(new Fields("predicates-eval")), Fields.SWAP);
    previous[0] = new Each(previous[0], Analytics.getTailsFields(Linksets.class).get("eval-linkset"), new Identity());

    /*
     * Replace the predicates-eval associated to a dst-eval blank
     */
    Pipe blankDst = new Pipe("blank-dst", previous[0]);
    blankDst = new Each(blankDst, new Fields("src-eval", "dst-eval", "predicates-eval"), new Not(new BlankFilter(false)));
    blankDst = new AggregateBy(blankDst, new Fields("domain", "src-eval", "dst-eval"),
      new CompactionBy(new Fields("predicates-gold"), new Fields("predicates-eval")));
    final Fields decDst = new Fields(
      "domain", "src-gold", "src-count-gold", "dst-gold", "dst-count-gold", "predicates-gold",
      "src-eval", "src-eval-count", "dst-eval", "dst-eval-count", "p_eval_l",
      "domain-r", "src-eval-r", "dst-eval-r", "p_eval_r"
    );
    previous[0] = new CoGroup("connectivity-assembly", previous[0], new Fields("domain", "src-eval", "dst-eval"),
      blankDst, new Fields("domain", "src-eval", "dst-eval"), decDst, new LeftJoin());
    previous[0] = new Each(previous[0], new Fields("p_eval_l", "p_eval_r"),
      new BlankSwitch(new Fields("predicates-eval")), Fields.SWAP);
    previous[0] = new Each(previous[0], Analytics.getTailsFields(Linksets.class).get("eval-linkset"), new Identity());

    /*
     * Compute the true-false positive links
     */
    // here, only false positives for connected gold clusters
    previous[0] = new Each(previous[0], new ConnectedTFPositive(new Fields("true", "false")), Fields.ALL);
    previous[0] = new Each(previous[0], new Fields("domain", "src-gold", "src-eval", "src-eval-count",
      "dst-eval", "dst-eval-count", "predicates-eval", "true", "false"), new Identity());
    // here, false positives for not-connected gold clusters (but connected in eval)
    previous[0] = new Each(previous[0], new Fields("predicates-eval"),
      new InnerTupleSize(new Fields("predicates-eval")), Fields.SWAP);
    // Sort on dst-eval. This allows to process in sequence the false positive between each pair of eval clusters.
    previous[0] = new GroupBy(previous[0], new Fields("domain", "src-eval"), new Fields("dst-eval"));
    previous[0] = new Every(previous[0], new Fields("src-gold", "src-eval", "src-eval-count", "dst-eval",
      "dst-eval-count", "predicates-eval", "true", "false"),
      new FalsePositiveBuffer(new Fields("cid-eval", "cid-gold", "size-eval", "true", "false")), Fields.SWAP);
    previous[0] = new Each(previous[0], Analytics.getTailFields(this), new Identity());
    previous[0] = new AggregateBy(previous[0], new Fields("domain", "cid-eval", "cid-gold", "size-eval"),
      new SumBy(new Fields("true"), new Fields("true"), Long.class),
      new SumBy(new Fields("false"), new Fields("false"), Long.class)
    );
    return previous[0];
  }

}
