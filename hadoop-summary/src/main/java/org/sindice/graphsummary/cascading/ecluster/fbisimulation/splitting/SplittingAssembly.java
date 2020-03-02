/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster.fbisimulation.splitting;

import static org.sindice.core.analytics.util.AnalyticsCounters.TUPLES_WRITTEN;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.sindice.core.analytics.cascading.operation.CompactionBy;
import org.sindice.core.analytics.cascading.operation.ExpansionFunction;
import org.sindice.graphsummary.cascading.ecluster.fbisimulation.UpdateCidFunction;
import org.sindice.graphsummary.cascading.ecluster.fbisimulation.adjacency.AdjacencyListAssembly;

import cascading.operation.Identity;
import cascading.operation.filter.FilterNotNull;
import cascading.operation.filter.FilterNull;
import cascading.operation.state.Counter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.joiner.LeftJoin;
import cascading.tuple.Fields;

/**
 * The {@link SplittingAssembly} class splits clusters that are not stable in the current partition of the data graph.
 * <p>
 * The splitting logic is done in {@link SplitBuffer}.
 * <p>
 * This {@link SubAssembly} has 2 heads:
 * <ul>
 * <li><b>partition-old</b>: it contains the current partition of the data graph; and</li>
 * <li><b>splits-old</b>: it contains the set of clusters in the current partition
 * that were split in the last iteration.</li>
 * </ul>
 * At the first iteration, the splits-old head contains the same data as the partition-old head.
 * <p>
 * This {@link SubAssembly} has 2 tails:
 * <ul>
 * <li><b>partition-new</b>: the new partition of the data graph, after performing splitting operations if any; and</li>
 * <li><b>splits-new</b>: the set of clusters that were split in this iteration.</li>
 * </ul>
 * <p>
 * The fields of heads and tails follow the format described in {@link AdjacencyListAssembly}.
 */
@AnalyticsName(value="splitting-assembly")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(name="partition-old", fields={ "domain", "entity", "incoming", "cid" }),
  @AnalyticsPipe(name="splits-old", fields={ "domain", "entity", "incoming", "cid" })
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(name="partition-new", fields={ "domain", "entity", "incoming", "cid" }),
  @AnalyticsPipe(name="splits-new", fields={ "domain", "entity", "incoming", "cid" })
})
public class SplittingAssembly
extends AnalyticsSubAssembly {

  private static final long serialVersionUID = -7268742450183390161L;

  public SplittingAssembly() {
    super();
  }

  public SplittingAssembly(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    final Fields tail = Analytics.getTailsFields(this).get("partition-new");

    Pipe in = new Pipe("splits-in", previous[1]);
    final Fields inFields = new Fields("incoming", "cid");
    in = new Each(in, inFields, new ExpansionFunction(inFields));
    in = new AggregateBy(in, new Fields("incoming"), new CompactionBy(new Fields("cid")));
    in = new Each(in, new Counter(JOB_ID, name + " - Incoming"));

    //Thanks to the CIDAggregateBy, we have unique keys on both sides of the join.
    Pipe join = new CoGroup("incoming-set-join", previous[0], new Fields("entity"), in, new Fields("incoming"),
      new Fields("domain", "entity", "incoming", "cid", "entity-right", "cid-right"), new LeftJoin());
    // Sort on the cid-right field, in reverse order
    // The reason is BytesStreamComparator put first null values. Since we set here the sorting in reverse and that
    // we get a null value for the first cid-right value, then it means all of them are null.
    join = new GroupBy(join, new Fields("domain", "cid"), new Fields("cid-right"), true);
    join = new Every(join, new SplitBuffer(new Fields("flag", "domain", "entity", "incoming", "cid", "cid-right")),
      Fields.RESULTS);
    join = new Each(join, new Fields("cid", "cid-right"), new UpdateCidFunction(new Fields("cid")), Fields.SWAP);
    join = new Each(join, new Counter(JOB_ID, TUPLES_WRITTEN + name));

    // Partition
    Pipe partition = new Pipe("partition-new", join);
    partition = new Each(partition, new Fields("flag"), new FilterNull());
    partition = new Each(partition, tail, new Identity());
    partition = new Each(partition, new Counter(JOB_ID, TUPLES_WRITTEN + name + "_partition"));

    // Splits
    Pipe splits = new Pipe("splits-new", join);
    splits = new Each(splits, new Fields("flag"), new FilterNotNull());
    splits = new Each(splits, tail, new Identity());
    splits = new Each(splits, new Counter(JOB_ID, TUPLES_WRITTEN + name + "_splits"));

    return Pipe.pipes(partition, splits);
  }

}
