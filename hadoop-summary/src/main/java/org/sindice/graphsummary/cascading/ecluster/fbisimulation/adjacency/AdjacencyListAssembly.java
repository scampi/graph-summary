/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster.fbisimulation.adjacency;

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
import org.sindice.graphsummary.cascading.ecluster.fbisimulation.splitting.SplittingAssembly;
import org.sindice.graphsummary.cascading.ecluster.ioproperties.IOPropertiesClusterGeneratorSubAssembly;
import org.sindice.graphsummary.cascading.entity.GetClusterGraph;

import cascading.operation.Identity;
import cascading.operation.filter.FilterNotNull;
import cascading.operation.filter.FilterNull;
import cascading.operation.state.Counter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.Unique;
import cascading.pipe.joiner.LeftJoin;
import cascading.tuple.Fields;

/**
 * The {@link AdjacencyListAssembly} prepares the data for the processing by {@link SplittingAssembly}.
 * <p>
 * This {@link SubAssembly} has 3 tails:
 * <ul>
 * <li><b>sources</b>: it contains the set of entities that have no incoming link;</li>
 * <li><b>sinks</b>: it contains the set of entities that have no outgoing link; and</li>
 * <li><b>intra</b>: it contains the rest of the entities.</li>
 * </ul>
 * Entities that are sources or sinks are filtered from the {@link SplittingAssembly} operation.
 * Cluster identifiers of sink entities won't change, and the cluster identifier of source entities can be updated
 * only at the last moment, when no more {@link SplittingAssembly splitting operation} is required.
 * <p>
 * Each tail has the same format, composed of 4 fields:
 * <ul>
 * <li><b>domain</b>: the domain of the document where the entity was found;</li>
 * <li><b>entity</b>: the entity identifier;</li>
 * <li><b>incoming</b>: the set of incoming entities, i.e., entities that point to this entity; and</li>
 * <li><b>cid</b>: the cluster identifier of this entity.</li>
 * </ul>
 * The incoming field of the sources and sinks tails are <code>null</code>, since they are no more needed
 * after this assembly completes. The reason for the sinks tail, the identifier of clusters that point
 * to a sink cluster is already updated in this assembly.
 */
@AnalyticsName(value="adjacency-list")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(from=GetClusterGraph.class)
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(name="intra", fields={ "domain", "entity", "incoming", "cid" }),
  @AnalyticsPipe(name="sources", fields={ "domain", "entity", "incoming", "cid" }),
  @AnalyticsPipe(name="sinks", fields={ "domain", "entity", "incoming", "cid" })
})
public class AdjacencyListAssembly
extends AnalyticsSubAssembly {

  private static final long serialVersionUID = -3373779157523105986L;

  public AdjacencyListAssembly() {
    super();
  }

  public AdjacencyListAssembly(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    final Fields tail = Analytics.getTailFields(this);

    final Fields adjFields = new Fields("domain", "entity", "outgoing", "cid");
    /*
     * Consider incoming links labels. This is to ensure that for every edge of an entity A,
     * the entity B has also those paths.
     */
    Pipe entity = new IOPropertiesClusterGeneratorSubAssembly(previous, true);
    entity = new Each(entity, new AdjacencyListFunction(adjFields));

    // Sinks
    Pipe maybeSinks = new Pipe("maybeSinks", entity);
    maybeSinks = new Each(maybeSinks, new Fields("domain", "entity"), new Identity());
    maybeSinks = new Unique(maybeSinks, new Fields("domain", "entity"));
    maybeSinks = new CoGroup("sink-join", entity, new Fields("outgoing"), maybeSinks, new Fields("entity"),
      new Fields("domain", "entity", "outgoing", "cid", "domain-r", "entity-r"),
      new LeftJoin());
    maybeSinks = new AggregateBy(maybeSinks, new Fields("domain", "entity", "cid"),
      new CompactionBy(new Fields("entity-r"), new Fields("outgoing")),
      new SinkAggregateBy(new Fields("entity-r"), new Fields("is-sink")));

    Pipe sinksPipe = new Pipe("sinks-pipe", maybeSinks);
    sinksPipe = new Each(sinksPipe, new Fields("is-sink"), new FilterNotNull());
    // The field "is-sink" is renamed to incoming, in order to match the field declaration
    // The incoming field is null. This is OK since it is not needed anymore
    sinksPipe = new Each(sinksPipe, new Fields("domain", "entity", "is-sink", "cid"), new Identity(tail));
    sinksPipe = new Each(sinksPipe, new Counter(JOB_ID, TUPLES_WRITTEN + "-sinks"));
    sinksPipe = new Each(sinksPipe, new Counter(JOB_ID, TUPLES_WRITTEN + name));

    /*
     * Update the CID label of entities connected to a leaf.
     * The leaves can then be left out from the computation.
     */
    Pipe sinksUpdate = new Pipe("sinks-update", maybeSinks);
    sinksUpdate = new Each(sinksUpdate, new Fields("is-sink"), new FilterNull());
    sinksUpdate = new Each(sinksUpdate, new Fields("outgoing", "domain", "entity", "cid"),
      new ExpansionFunction(new Fields("outgoing", "domain", "entity", "cid")));
    sinksUpdate = new CoGroup("sink-update", sinksUpdate, new Fields("outgoing"), sinksPipe, new Fields("entity"),
      new Fields("outgoing", "domain", "entity", "cid", "domain-r", "entity-r", "incoming-r", "cid-right"),
      new LeftJoin());
    sinksUpdate = new AggregateBy(sinksUpdate, new Fields("domain", "entity", "cid"),
      new CompactionBy(new Fields("cid-right")),
      new CompactionBy(new Fields("outgoing")),
      new SinkAggregateBy(new Fields("outgoing"), new Fields("is-sink")));
    sinksUpdate = new Each(sinksUpdate, new Fields("cid", "cid-right"),
      new UpdateCidFunction(new Fields("cid")), Fields.SWAP);
    sinksUpdate = new Each(sinksUpdate, adjFields, new Identity());

    // Aggregate the incoming set of entities
    Pipe in = new Pipe("incoming", sinksUpdate);
    in = new Each(in, new Fields("outgoing", "entity"),
      new ExpansionFunction(new Fields("outgoing", "entity")));
    in = new AggregateBy(in, new Fields("outgoing"), new CompactionBy(new Fields("entity")));
    in = new Each(in, new Fields("outgoing", "entity"), new Identity(new Fields("entity", "incoming")));

    sinksUpdate = new CoGroup("IncomingSet-join", sinksUpdate, new Fields("entity"), in, new Fields("entity"),
      new Fields("domain", "entity", "outgoing", "cid", "entity-r", "incoming"), new LeftJoin());
    sinksUpdate = new Each(sinksUpdate, tail, new Identity());

    // Sources
    Pipe sources = new Pipe("sources", sinksUpdate);
    sources = new Each(sources, new Fields("incoming"), new FilterNotNull());
    sources = new Each(sources, new Counter(JOB_ID, TUPLES_WRITTEN + "-sources"));
    sources = new Each(sources, new Counter(JOB_ID, TUPLES_WRITTEN + name));

    // Intra
    Pipe intra = new Pipe("intra", sinksUpdate);
    intra = new Each(intra, new Fields("incoming"), new FilterNull());
    intra = new Each(intra, new Counter(JOB_ID, TUPLES_WRITTEN + "-intra"));
    intra = new Each(intra, new Counter(JOB_ID, TUPLES_WRITTEN + name));

    Pipe sinks = new Pipe("sinks", sinksPipe);

    return Pipe.pipes(intra, sources, sinks);
  }

}
