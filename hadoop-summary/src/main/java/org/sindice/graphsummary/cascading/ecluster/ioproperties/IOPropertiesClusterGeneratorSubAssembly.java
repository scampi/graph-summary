/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster.ioproperties;

import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.graphsummary.cascading.ecluster.ClusterSubAssembly;
import org.sindice.graphsummary.cascading.ecluster.properties.PropertiesCreateClusterIDFunction;
import org.sindice.graphsummary.cascading.ecluster.typesproperties.TypesPropertiesCreateClusterIDFunction;
import org.sindice.graphsummary.cascading.entity.GetClusterGraph;
import org.sindice.graphsummary.cascading.entity.GetClusterGraph.PreProcessing;

import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

/**
 * This {@link ClusterSubAssembly} generate a cluster identifier based on:
 * <ul>
 * <li>the type feature;</li>
 * <li>the outgoing attributes feature; and</li>
 * <li>the incoming attributes feature.</li>
 * </ul>
 * The type feaature is taken into account only if the argument passed to
 * {@link IOPropertiesClusterGeneratorSubAssembly} is <code>true</code>.
 * <p>
 * In order to get the incoming edges, this algorithm requires that {@link GetClusterGraph} was run
 * with the {@link PreProcessing#IO_EDGES_AGGREGATE} option.
 */
public class IOPropertiesClusterGeneratorSubAssembly extends ClusterSubAssembly {

  private static final long  serialVersionUID = -5994460027793900505L;

  /**
   * If withClass is <code>true</code>, the type feature of the entity is considered in the clustering.
   */
  public IOPropertiesClusterGeneratorSubAssembly(final boolean withClass) {
    super(withClass);
  }

  public IOPropertiesClusterGeneratorSubAssembly(Pipe[] lhs, Object...args) {
    super(lhs, args);
  }

  @Override
  protected Pipe cluster(Pipe entity, Object... args) {
    final Fields tail = Analytics.getTailFields(this);
    if ((Boolean) args[0]) {
      entity = new Each(entity, new Fields("spo-in", "spo-out"),
        new TypesPropertiesCreateClusterIDFunction(new Fields("cluster-id")), Fields.ALL);
    } else {
      entity = new Each(entity, new Fields("spo-in", "spo-out"),
        new PropertiesCreateClusterIDFunction(new Fields("cluster-id")), Fields.ALL);
    }

    entity = new Each(entity, Analytics.getTailFields(this), new Identity(tail));
    return entity;
  }

}
