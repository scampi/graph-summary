/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.statistics;

import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.sindice.graphsummary.cascading.properties.GetPropertiesGraph;

import cascading.operation.Identity;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;

/**
 * This {@link SubAssembly} computes the number of edges and nodes per domain in a summary.
 */
@AnalyticsName(value="summary-volume")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(from=GetPropertiesGraph.class)
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "domain", "nb-nodes", "count-edges" })
})
public class SummaryVolume extends AnalyticsSubAssembly {

  private static final long  serialVersionUID  = 384097574852869386L;

  public SummaryVolume() {
    super();
  }

  public SummaryVolume(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    Pipe size = new Pipe("size", previous[0]);
    size = new Each(size, new CountEdgesFunction(new Fields("count-edges")), Fields.ALL);
    size = new SumBy(size, new Fields("domain"),
      new Fields("count-edges"), new Fields("count-edges"), Long.class);
    size = new Each(size, new Fields("domain", "count-edges"), new Identity());

    Pipe order = new Pipe("order", previous[0]);
    order = new CountBy(order, new Fields("domain"), new Fields("nb-nodes"));

    final Fields dec = new Fields("domain", "count-edges", "domain-r", "nb-nodes");
    Pipe volume = new CoGroup("summary-volume", size, new Fields("domain"), order, new Fields("domain"), dec);
    volume = new Each(volume, Analytics.getTailFields(this), new Identity());

    return volume;
  }

}
