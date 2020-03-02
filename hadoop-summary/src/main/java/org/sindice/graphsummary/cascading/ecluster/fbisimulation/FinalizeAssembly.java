/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster.fbisimulation;

import static org.sindice.core.analytics.util.AnalyticsCounters.TUPLES_READ;
import static org.sindice.core.analytics.util.AnalyticsCounters.TUPLES_WRITTEN;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.sindice.graphsummary.cascading.ecluster.ClusterSubAssembly;
import org.sindice.graphsummary.cascading.ecluster.fbisimulation.splitting.SplittingAssembly;
import org.sindice.graphsummary.cascading.entity.GetClusterGraph;

import cascading.operation.Identity;
import cascading.operation.state.Counter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

/**
 * The {@link FinalizeAssembly} class appends the cluster identifier computed by the iterations of
 * {@link SplittingAssembly splitting operations} to the {@link GetClusterGraph entity tuple}.
 */
@AnalyticsName(value="finalize")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(from=GetClusterGraph.class),
  @AnalyticsPipe(name="partition", from=SplittingAssembly.class, sinkName="partition-new")
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "domain", "subject-hash", "spo-in", "spo-out", "cluster-id" })
})
public class FinalizeAssembly
extends AnalyticsSubAssembly {

  private static final long serialVersionUID = 6967681650621038131L;

  public FinalizeAssembly() {
    super();
  }

  public FinalizeAssembly(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    Pipe cg = new Each(previous[0], new Counter(JOB_ID, TUPLES_READ + name));

    Pipe partitionQ = new Each(previous[1], new Counter(JOB_ID, TUPLES_READ + name));
    partitionQ = new Each(partitionQ, new Fields("domain", "entity", "cid"), new Identity());

    Pipe pipe = new CoGroup(name, cg, new Fields("domain", "subject-hash"),
      partitionQ, new Fields("domain", "entity"),
      new Fields("domain", "subject-hash", "spo-in", "spo-out", "domain-right", "entity", "cid"));

    final Fields intern = new Fields("domain", "subject-hash", "spo-in", "spo-out", "cid");
    pipe = new Each(pipe, intern, new Identity(Analytics.getTailFields(ClusterSubAssembly.class)));

    pipe = new Each(pipe, new Counter(JOB_ID, TUPLES_WRITTEN + name));
    return pipe;
  }

}
