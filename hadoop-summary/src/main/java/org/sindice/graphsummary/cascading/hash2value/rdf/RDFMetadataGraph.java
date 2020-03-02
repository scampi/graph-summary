/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.hash2value.rdf;

import static org.sindice.core.analytics.util.AnalyticsCounters.TUPLES_READ;
import static org.sindice.core.analytics.util.AnalyticsCounters.TUPLES_WRITTEN;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;
import static org.sindice.graphsummary.cascading.SummaryCounters.SUMMARY_ID;
import static org.sindice.graphsummary.cascading.SummaryCounters.TRIPLES;

import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.sindice.graphsummary.cascading.properties.GetPropertiesGraph;

import cascading.operation.state.Counter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;

/**
 * Parse data from Properties branch and create associated node and edge
 * N-triples in the output RDF
 * 
 * @author Pierre Bailly-Ferry <pierre.bailly@deri.org>
 */
@AnalyticsName(value="RDF-Metadata-Summary")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(from=GetPropertiesGraph.class)
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "rdf" })
})
public class RDFMetadataGraph extends AnalyticsSubAssembly {

  private static final long  serialVersionUID  = -4355735413534565104L;

  public RDFMetadataGraph() {
    super();
  }

  public RDFMetadataGraph(Pipe[] pipe) {
    super(pipe, (Object[]) null);
  }

  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    Pipe pipe = new Each(previous[0], new Counter(JOB_ID, TUPLES_READ + name));

    pipe = new Each(pipe, Analytics.getHeadFields(this), new RDFPropertiesGraphFunction(Analytics.getTailFields(this)));
    pipe = new Each(pipe, new Counter(SUMMARY_ID, TRIPLES));

    pipe = new Each(pipe, new Counter(JOB_ID, TUPLES_WRITTEN + name));
    return pipe;
  }

}
