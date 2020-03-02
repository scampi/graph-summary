/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster.types;

import org.sindice.graphsummary.cascading.ecluster.ClusterSubAssembly;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

/**
 * Sub assembly to create the CID. Here we create the cluster from the name of
 * all the types aggregated.
 * 
 * @author Pierre Bailly-Ferry <pierre.bailly@deri.org>
 */
public class TypesClusterSubAssembly extends ClusterSubAssembly {

  private static final long serialVersionUID = -8010226573202601351L;

  public TypesClusterSubAssembly() {
    super();
  }

  public TypesClusterSubAssembly(Pipe[] pipes) {
    super(pipes);
  }

  @Override
  protected Pipe cluster(Pipe entity, Object... args) {
    return new Each(entity, new Fields("spo-out"),
      new TypesCreateClusterIDFunction(new Fields("cluster-id")), Fields.ALL);
  }

}
