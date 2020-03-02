/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.rdf;

import org.sindice.core.analytics.cascading.AnalyticsParameters;

/**
 * Specify the format of the documents containing RDF data.
 * 
 * <ul>
 * <li><b>SINDICE_EXPORT</b>: the JSON format used in Sindice;</li>
 * <li><b>NTRIPLES</b>: RDF data in N-Triples format; and</li>
 * <li><b>NQUADS</b>: RDF data in N-Quads format.</li>
 * </ul>
 * 
 * <p>
 * 
 * When using <b>NTRIPLES</b>, the parameter {@link AnalyticsParameters#DEFAULT_DOMAIN} should also be used.
 * If unset and {@link AnalyticsParameters#CHECK_AUTH_TYPE} is <code>true</code>, it is very likely that all entities would be
 * marked as non-authoritative.
 */
public enum DocumentFormat {
  /**
   * The Sindice JSON document format
   */
  SINDICE_EXPORT,
  /**
   * Plain ntriples
   */
  NTRIPLES,
  /**
   * Plain nquads
   */
  NQUADS
}
