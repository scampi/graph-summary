/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster.namespace;

import org.openrdf.model.URI;
import org.sindice.core.analytics.rdf.AnalyticsClassAttributes;
import org.sindice.graphsummary.cascading.ecluster.ClusterSubAssembly;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

/**
 * This {@link ClusterSubAssembly} cluster entities based on the following features:
 * <ul>
 * <li><b>type</b>: the type of the entity, which predicated is identified by {@link AnalyticsClassAttributes};</li>
 * <li><b>attribute</b>: the label of the predicates associated to the entity;</li>
 * <li><b>namespace</b>: the namespace of the object {@link URI}.</li>
 * </ul>
 * The namespace of incoming objects can also be considered.
 */
public class NSClusterSubAssembly extends ClusterSubAssembly {

  private static final long serialVersionUID = -4253206494623963160L;

  /**
   * @param withIncoming if <code>true</code>, take also the namespace
   * of the incoming objects as an additional feature.
   */
  public NSClusterSubAssembly(final boolean withIncoming) {
    super(withIncoming);
  }

  public NSClusterSubAssembly(Pipe[] pipes, Object...args) {
    super(pipes, args);
  }

  @Override
  protected Pipe cluster(Pipe entity, Object... args) {
    return new Each(entity, new Fields("spo-in", "spo-out"),
      new NSCreateClusterIDFunction(new Fields("cluster-id"), (Boolean) args[0]), Fields.ALL);
  }

}
