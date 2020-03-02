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
import org.sindice.graphsummary.cascading.entity.GetClusterGraph;

import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

/**
 * This {@link AnalyticsSubAssembly} computes statistics about entities.
 * It reports in the field <b>stats</b> various statitics, in JSON format.
 * The reported numbers are the following:
 * <ul>
 * <li>the number of triples;</li>
 * <li>the cardinality of a predicate label;</li>
 * <li>the list of unique objects, and the number of times an object is linked to.</li>
 * </ul>
 */
@AnalyticsName(value="entity-distribution")
@AnalyticsHeadPipes(values={
    @AnalyticsPipe(from=GetClusterGraph.class)
})
@AnalyticsTailPipes(values={
    @AnalyticsPipe(fields={ "domain", "subject-hash", "stats" })
})
public class EntityDistribution
extends AnalyticsSubAssembly {

  private static final long serialVersionUID = -5118402084954772527L;

  public EntityDistribution() {
    super();
  }

  public EntityDistribution(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    previous[0] = new Each(previous[0], new EntityDistributionFunction(new Fields("stats")), Fields.ALL);
    previous[0] = new Each(previous[0], Analytics.getTailFields(this), new Identity());
    return previous[0];
  }

}
