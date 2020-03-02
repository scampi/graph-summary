/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.hash2value.rdf.filter;

import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.sindice.graphsummary.cascading.hash2value.rdf.DataGraphSummaryVocab;

import cascading.operation.Identity;
import cascading.operation.filter.FilterNotNull;
import cascading.operation.filter.FilterNull;
import cascading.operation.regex.RegexFilter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.joiner.LeftJoin;
import cascading.tuple.Fields;

/**
 * This {@link AnalyticsSubAssembly} filters nodes from the graph summary that matches a SPARQL query. The connected
 * incoming and outgoing edges are also removed.
 */
@AnalyticsName(value="Filter-Summary-Graph")
@AnalyticsHeadPipes(values={ @AnalyticsPipe(fields={ "quad" }) })
@AnalyticsTailPipes(values={ @AnalyticsPipe(fields={ "quad" }) })
public class NodeFilterSummaryGraph
extends AnalyticsSubAssembly {

  private static final long  serialVersionUID  = 3184740970699095455L;

  static final String        NODE_PREFIX       = DataGraphSummaryVocab.ANY23_PREFIX + "node";
  static final String        EDGE_PREFIX       = DataGraphSummaryVocab.ANY23_PREFIX + "edge";

  public NodeFilterSummaryGraph(final String filterQuery) {
    super(filterQuery);
  }

  public NodeFilterSummaryGraph(final Pipe[] pipes, final Object... args) {
    super(pipes, args);
  }

  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    final Fields sel = Analytics.getHeadFields(this);
    final Fields dec = Analytics.getTailFields(this);

    Pipe quad = new Each(previous[0], new Quad2TripleFunction(sel));

    final Fields nodesFields = new Fields("node-hash", "quad");
    final Fields edgesFields = new Fields("source", "target", "quad");

    /*
     * Nodes
     */
    Pipe nodes = new Pipe("nodes", quad);
    nodes = new Each(nodes, new RegexFilter("^<" + NODE_PREFIX));
    nodes = new Each(nodes, new ExtractNodeHash(nodesFields));
    nodes = new AggregateBy(nodes, new Fields("node-hash"), new GraphFilterAggregateBy(new Fields("quad"),
      new Fields("keep", "data"), (String) args[0]));

    // nodes that are NOT matched by the filter query
    Pipe validNodes = new Pipe("valid-nodes", nodes);
    validNodes = new Each(validNodes, new FilterNull());
    validNodes = new Each(validNodes, new Fields("data"), new Identity(dec));

    // nodes that ARE matched by the filter query
    Pipe filteredNodes = new Pipe("filtered-nodes", nodes);
    filteredNodes = new Each(filteredNodes, new Fields("keep"), new FilterNotNull());
    filteredNodes = new Each(filteredNodes, new Fields("data"), new Identity());

    /*
     * Edges
     */
    Pipe edges = new Pipe("edges", quad);
    edges = new Each(edges, new RegexFilter("^<" + EDGE_PREFIX));
    edges = new Each(edges, new ExtractEdgeSourceTargetHash(edgesFields));

    // remove edges which source is a filtered node
    Pipe join = new CoGroup(edges, new Fields("source"), filteredNodes, new Fields("data"),
      new Fields("source", "target", "quad", "data"), new LeftJoin());
    join = new Each(join, new Fields("data"), new FilterNotNull());
    join = new Each(join, new Fields("target", "quad"), new Identity());

    // remove edges which target is a filtered node
    join = new CoGroup(join, new Fields("target"), filteredNodes, new Fields("data"),
      new Fields("target", "quad", "data"), new LeftJoin());
    join = new Each(join, new Fields("data"), new FilterNotNull());
    join = new Each(join, dec, new Identity());

    /*
     * Retrieve the graph that were not matched.
     */
    join = new Merge(name, Pipe.pipes(join, validNodes));
    join = new Each(join, new Triple2QuadFunction(sel));
    return join;
  }

}
