/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.properties;

import static org.sindice.core.analytics.util.AnalyticsCounters.TUPLES_READ;
import static org.sindice.core.analytics.util.AnalyticsCounters.TUPLES_WRITTEN;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.sindice.graphsummary.cascading.ecluster.ClusterSubAssembly;
import org.sindice.graphsummary.cascading.ecluster.ClusteringFactory.ClusteringAlgorithm;

import cascading.operation.state.Counter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;

/**
 * Flow to get types, the class-attributes and the properties from the output of
 * ClusterGeneratorGraph.
 * 
 * Two steps: First parse the <spo> with SPO2HashPropertiesFunction, then
 * aggregate the cluster-id with ClusterIDAggregateBy.
 * 
 * Input:
 * 
 * [ domain | subject-hash | spo | cluster-id ]
 * 
 * Output:
 * 
 * [ domain | cluster-id | types | properties | c-count ]
 * 
 * @author Pierre Bailly-Ferry <pierre.bailly@deri.org>
 */
@AnalyticsName(value="properties-flow")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(from=ClusterSubAssembly.class)
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "domain", "cluster-id", "types", "properties", "c-count" })
})
public class GetPropertiesGraph extends AnalyticsSubAssembly {

  private static final long serialVersionUID = 384097574852869386L;

  /** Flag a tuple that has information about a type of the cluster */
  public static final int   TYPE             = 0;
  /** Flag a tuple that has information about an attribute of the cluster */
  public static final int   ATTRIBUTE        = 1;
  /** Flag a tuple that is about the entity. It allows to count the number of entities. */
  public static final int   ENTITY           = 2;

  /**
   * This enum allows to customise the {@link GetPropertiesGraph} flow.
   */
  public enum PropertiesProcessing {
    /**
     * Default processing, which consists in aggregating the types and attributes
     * information about a cluster.
     */
    DEFAULT,
    /**
     * This processing is to be used with the {@link ClusteringAlgorithm#SINGLE_TYPE}.
     * It removes type labels which are not equal to the label with which the cluster is defined.
     */
    SINGLE_TYPE
  }

  public GetPropertiesGraph(PropertiesProcessing proc) {
    super(proc);
  }

  public GetPropertiesGraph(Pipe[] pipes, Object...args) {
    super(pipes, args);
  }

  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    Pipe pipe = new Each(previous[0], new Counter(JOB_ID, TUPLES_READ + name));

    final Fields schemaFields = new Fields("flag", "label", "value", "entity");
    switch ((PropertiesProcessing) args[0]) {
      case DEFAULT:
        pipe = new Each(pipe, new Fields("spo-out"), new SchemaFunction(schemaFields), Fields.SWAP);
        break;
      case SINGLE_TYPE:
        pipe = new Each(pipe, new Fields("cluster-id", "spo-out"),
          new SingleTypeSchemaFunction(schemaFields), Fields.ALL);
        break;
      default:
        throw new EnumConstantNotPresentException(PropertiesProcessing.class, args[0].toString());
    }
    pipe = new AggregateBy(pipe, new Fields("domain", "cluster-id"),
        new ClusterIDAggregateBy(schemaFields, new Fields("types", "properties")),
        new SumBy(new Fields("entity"), new Fields("c-count"), Long.class));

    pipe = new Each(pipe, new Counter(JOB_ID, TUPLES_WRITTEN + name));
    return pipe;
  }

}
