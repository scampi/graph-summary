/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */

package org.sindice.graphsummary.cascading.ecluster.singletype;

import org.sindice.graphsummary.cascading.ecluster.ClusterSubAssembly;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

/**
 * Function to create the CID, on cluster ID per type.
 * 
 * @author artbau
 */
public class SingleTypeClusterSubAssembly
extends ClusterSubAssembly {

  private static final long serialVersionUID = 1649128264895831966L;

  public SingleTypeClusterSubAssembly() {
    super();
  }

  public SingleTypeClusterSubAssembly(Pipe[] pipes) {
    super(pipes);
  }

  @Override
  protected Pipe cluster(Pipe entity, Object... args) {
    return new Each(entity, new Fields("spo-out"),
      new SingleTypeCreateClusterIDFunction(new Fields("cluster-id")), Fields.ALL);
  }

}
