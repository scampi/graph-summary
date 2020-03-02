/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster.properties;

import org.sindice.graphsummary.cascading.ecluster.ClusterSubAssembly;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

/**
 * Sub assembly to create the CID. Here we create the cluster from the name of
 * all the properties aggregated. Input: [ domain | domain-text | subject-hash |
 * spo ] Output: [ domain | domain-text | subject-hash | spo | cluster-id ]
 * 
 * @author Pierre Bailly-Ferry <pierre.bailly@deri.org>
 */
public class PropertiesClusterSubAssembly
extends ClusterSubAssembly {

  private static final long serialVersionUID = 2035651815635808787L;

  public PropertiesClusterSubAssembly() {
    super();
  }

  public PropertiesClusterSubAssembly(Pipe[] pipes) {
    super(pipes);
  }

  @Override
  protected Pipe cluster(Pipe entity, Object... args) {
    return new Each(entity, new Fields("spo-in", "spo-out"),
      new PropertiesCreateClusterIDFunction(new Fields("cluster-id")), Fields.ALL);
  }

}
