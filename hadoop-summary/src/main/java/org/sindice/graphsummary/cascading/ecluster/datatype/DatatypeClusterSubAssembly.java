/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster.datatype;

import org.openrdf.model.Literal;
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
 * <li><b>datatype</b>: the datatype of the {@link Literal}.</li>
 * </ul>
 * If a {@link Literal} has no datatype, we use instead the default
 * {@link DatatypeCreateClusterIDFunction#XSD_STRING xsd:string} datatype.
 */
public class DatatypeClusterSubAssembly extends ClusterSubAssembly {

  private static final long serialVersionUID = -4253206494623963160L;

  public DatatypeClusterSubAssembly() {
    super();
  }

  public DatatypeClusterSubAssembly(Pipe[] pipes) {
    super(pipes);
  }

  @Override
  protected Pipe cluster(Pipe entity, Object... args) {
    return new Each(entity, new Fields("spo-out"),
      new DatatypeCreateClusterIDFunction(new Fields("cluster-id")), Fields.ALL);
  }

}
