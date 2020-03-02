/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.hash2value.rdf.filter;

import java.nio.charset.Charset;

import cascading.pipe.assembly.AggregateBy;
import cascading.tuple.Fields;


/**
 * This {@link AggregateBy} flags for removal the nodes of the Graph Summary
 * that matches the SPARQL query.
 */
public class GraphFilterAggregateBy
extends AggregateBy {

  /** Report the number of occurrences of ranges of triples in a node */
  public static final String NB_TRIPLES       = "NB_TRIPLES";
  /** Report the number of filtered nodes */
  public static final String FILTERED         = "FILTERED_NODES";
  /** A newline '\n' character as a byte array */
  public static final byte[] NEWLINE          = "\n".getBytes(Charset.forName("UTF-8"));

  private static final long serialVersionUID = -2511002871402368776L;

  /**
   * Create a new {@link GraphFilterAggregateBy} instance that will filter nodes matching the given query.
   * @param argField The arguments {@link Fields}
   * @param outField The declared {@link Fields}
   * @param filterQuery The WHERE clause of the SPARQL query
   */
  public GraphFilterAggregateBy(final Fields argField,
                                final Fields outField,
                                final String filterQuery) {
    super(argField, new GraphFilterAggregatePartials(argField), new GraphFilterAggregate(outField, filterQuery));
  }

}
