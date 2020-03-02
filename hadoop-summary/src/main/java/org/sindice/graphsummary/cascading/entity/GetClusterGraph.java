/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.entity;

import static org.sindice.core.analytics.util.AnalyticsCounters.TUPLES_READ;
import static org.sindice.core.analytics.util.AnalyticsCounters.TUPLES_WRITTEN;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import org.openrdf.model.BNode;
import org.sindice.analytics.entity.EntityDescription;
import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.sindice.core.analytics.rdf.RDFParser;
import org.sindice.graphsummary.cascading.ecluster.ClusteringFactory.ClusteringAlgorithm;

import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.operation.filter.FilterNull;
import cascading.operation.state.Counter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.tuple.Fields;

/**
 * This {@link AnalyticsSubAssembly} extracts the description of an entity.
 * The description consists of the outgoing edges of the entity, and if necessary for the {@link ClusteringAlgorithm},
 * the incoming edges also.
 * <p>
 * This flow apply various cleaning rules on the entity description:
 * <ul>
 * <li>Removes statements that cannot be parsed with {@link RDFParser#parseStatement(String)}</li>
 * <li>Turns {@link BNode}s into unique identifiers</li>
 * <li>Clean the type features in {@link FilterClasses}</li>
 * </ul>
 * <p>
 * It is possible to have a custom computation of the {@link EntityDescription entity description}
 * through {@link PreProcessing}.
 */
@AnalyticsName(value="get-cluster-flow")
@AnalyticsHeadPipes(values={
    @AnalyticsPipe(fields={ "value" })
})
@AnalyticsTailPipes(values={
    @AnalyticsPipe(fields={ "domain", "subject-hash", "spo-in", "spo-out" })
})
public class GetClusterGraph extends AnalyticsSubAssembly {

  private static final long  serialVersionUID  = 384097574852869386L;

  /**
   * This enum lists possible pre-processing operations to create the {@link EntityDescription}.
   */
  public enum PreProcessing {
    /**
     * The outgoing edges only are part of the entity description.
     * <p>
     * For example, this is used in combination of clustering algorithms requiring
     * only outgoing edges, such as {@link ClusteringAlgorithm#TYPES}.
     */
    O_EDGES_AGGREGATE,
    /**
     * The incoming and outgoing edges are part of the entity description.
     * In this approach, the subject label of the incoming edges are dropped.
     * <p>
     * For example, this is used in combination of clustering algorithms requiring
     * incoming + outgoing edges labels, such as {@link ClusteringAlgorithm#IO_PROPERTIES}.
     */
    IO_EDGES_AGGREGATE,
    /**
     * The incoming and outgoing edges are part of the entity description.
     * In this approach, the predicate label of the incoming edges is dropped.
     * <p>
     * For example, this is used in combination of clustering algorithms requiring
     * incoming + outgoing edges, with a focus on the incoming object label,
     * such as {@link ClusteringAlgorithm#IO_NAMESPACE}.
     */
    IO_OBJECT_EDGES_AGGREGATE
  }

  public GetClusterGraph(PreProcessing optim) {
    super(optim);
  }

  public GetClusterGraph(Pipe[] pipes, Object... args) {
    super(pipes, args);
  }

  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    Pipe pipe = new Each(previous[0], Analytics.getHeadFields(this),
        new Counter(JOB_ID, TUPLES_READ + name));

    final Fields spo = new Fields("s", "p", "o");
    pipe = new Each(pipe, new StatementFunction(new Fields("domain", "subject-hash", "s", "p", "o")));
    pipe = new Each(pipe, spo, new FilterClasses(spo), Fields.SWAP);

    // Apply the appropriate entity description pre-processing
    switch (PreProcessing.valueOf(args[0].toString())) {
      case O_EDGES_AGGREGATE:
        pipe = new AggregateBy(pipe, new Fields("domain", "subject-hash"),
          new SubjectAggregateBy(spo, new Fields("spo-out")));
        pipe = new Each(pipe, new Insert(new Fields("spo-in"), new Object[] { null }), Fields.ALL);
        break;

      case IO_EDGES_AGGREGATE:
        pipe = new Each(pipe, new InverseEdgesDropSubject(new Fields("flag", "domain", "subject-hash", "s", "p", "o")));
        pipe = new AggregateBy(pipe, new Fields("domain", "subject-hash"),
          new SubjectAggregateBy(new Fields("flag", "s", "p", "o"), new Fields("spo-in"), InverseEdgesDropSubject.IN_FLAG),
          new SubjectAggregateBy(new Fields("flag", "s", "p", "o"), new Fields("spo-out"), InverseEdgesDropSubject.OUT_FLAG));
        pipe = new Each(pipe, new Fields("spo-out"), new FilterNull()); // sink entities
        break;

      case IO_OBJECT_EDGES_AGGREGATE:
        pipe = new Each(pipe, new InverseEdgesDropPredicate(new Fields("flag", "domain", "subject-hash", "s", "p", "o")));
        pipe = new AggregateBy(pipe, new Fields("domain", "subject-hash"),
          new SubjectAggregateBy(new Fields("flag", "s", "p", "o"), new Fields("spo-in"), InverseEdgesDropPredicate.IN_FLAG),
          new SubjectAggregateBy(new Fields("flag", "s", "p", "o"), new Fields("spo-out"), InverseEdgesDropPredicate.OUT_FLAG));
        pipe = new Each(pipe, new Fields("spo-out"), new FilterNull()); // sink entities
        break;

      default:
        throw new EnumConstantNotPresentException(PreProcessing.class, args[0].toString());
    }

    pipe = new Each(pipe, Analytics.getTailFields(this), new Identity());
    pipe = new Each(pipe, new Counter(JOB_ID, TUPLES_WRITTEN + name));
    return pipe;
  }

}
