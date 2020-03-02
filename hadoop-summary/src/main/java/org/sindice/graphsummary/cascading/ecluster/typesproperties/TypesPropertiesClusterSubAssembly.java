/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster.typesproperties;

import org.sindice.graphsummary.cascading.ecluster.ClusterSubAssembly;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

/**
 * Sub assembly to create the CID. Here we create the cluster from the name of
 * all the properties and all the types aggregated.
 * 
 * @author Pierre Bailly-Ferry <pierre.bailly@deri.org>
 */
public class TypesPropertiesClusterSubAssembly extends ClusterSubAssembly {

  private static final long serialVersionUID = -4253206494623963160L;

  public TypesPropertiesClusterSubAssembly() {
    super();
  }

  public TypesPropertiesClusterSubAssembly(Pipe[] pipes) {
    super(pipes);
  }

  @Override
  protected Pipe cluster(Pipe entity, Object... args) {
    return new Each(entity, new Fields("spo-in", "spo-out"),
      new TypesPropertiesCreateClusterIDFunction(new Fields("cluster-id")), Fields.ALL);
  }

}
