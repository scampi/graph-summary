/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.statistics;

import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.sindice.graphsummary.cascading.entity.GetClusterGraph;

import cascading.operation.Identity;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.filter.FilterNull;
import cascading.operation.function.UnGroup;
import cascading.pipe.Each;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.SumBy;
import cascading.pipe.assembly.Unique;
import cascading.tuple.Fields;

/**
 * This {@link SubAssembly} computes the average indegree and outdegree per domain of a data graph.
 */
@AnalyticsName(value="average-degree")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(from=GetClusterGraph.class)
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(name="avg-indegree", fields={ "domain", "avg-indegree" }),
  @AnalyticsPipe(name="avg-outdegree", fields={ "domain", "avg-outdegree" })
})
public class EntityAverageDegree extends AnalyticsSubAssembly {

  private static final long  serialVersionUID  = 384097574852869386L;

  public EntityAverageDegree() {
    super();
  }

  public EntityAverageDegree(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    previous[0] = new Each(previous[0], new EntityCountEdges(new Fields("o", "degree")), Fields.ALL);

    /*
     * Nb nodes
     */
    Pipe nbNodes = new Pipe("nb-nodes", previous[0]);
    nbNodes = new Each(nbNodes, new Fields("domain", "subject-hash", "o"),
      new UnGroup(new Fields("domain", "entity"), new Fields("domain"),
        new Fields[] { new Fields("subject-hash"), new Fields("o") }));
    nbNodes = new Each(nbNodes, new Fields("entity"), new FilterNull());
    nbNodes = new Unique(nbNodes, new Fields("domain", "entity"));
    nbNodes = new CountBy(nbNodes, new Fields("domain"), new Fields("entity"));

    /*
     * Out-degree
     */
    Pipe outDegree = new Pipe("outdegree", previous[0]);
    outDegree = new SumBy(outDegree, new Fields("domain", "subject-hash"),
      new Fields("degree"), new Fields("degree"), Long.class);
    outDegree = new SumBy(outDegree, new Fields("domain"),
      new Fields("degree"), new Fields("outdegree"), Long.class);
    final Fields decOut = new Fields("domain", "nbnodes", "domain-r", "outdegree");
    outDegree = new HashJoin(nbNodes, new Fields("domain"), outDegree, new Fields("domain"), decOut);
    outDegree = new Each(outDegree, new Fields("outdegree", "nbnodes"),
      new ExpressionFunction(new Fields("avg-outdegree"), "outdegree / nbnodes", Double.class), Fields.SWAP);
    outDegree = new Each(outDegree, tails.get("avg-outdegree"), new Identity());
    

    /*
     * In-degree
     */
    Pipe inDegree = new Pipe("indegree", previous[0]);
    inDegree = new Each(inDegree, new Fields("o"), new FilterNull());
    inDegree = new SumBy(inDegree, new Fields("domain", "o"),
      new Fields("degree"), new Fields("degree"), Long.class);
    inDegree = new SumBy(inDegree, new Fields("domain"),
      new Fields("degree"), new Fields("indegree"), Long.class);
    final Fields decIn = new Fields("domain", "nbnodes", "domain-r", "indegree");
    inDegree = new HashJoin(nbNodes, new Fields("domain"), inDegree, new Fields("domain"), decIn);
    inDegree = new Each(inDegree, new Fields("indegree", "nbnodes"),
      new ExpressionFunction(new Fields("avg-indegree"), "indegree / nbnodes", Double.class), Fields.SWAP);
    inDegree = new Each(inDegree, tails.get("avg-indegree"), new Identity());

    final Pipe avgInDegree = new Pipe("avg-indegree", inDegree);
    final Pipe avgOutDegree = new Pipe("avg-outdegree", outDegree);
    return Pipe.pipes(avgInDegree, avgOutDegree);
  }

}
