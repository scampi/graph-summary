/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.rdf;

/**
 * This enum defines the context URI of a summary graph.
 * <ul>
 * <li><b>SINDICE</b>: the context is equal to http://sindice.com/analytics; and</li>
 * <li><b>DATASET</b>: the context is equal to http://sindice.com/dataspace/default/domain/${DATASET-LABEL}, where the
 * dataset label is equal to the second-level domain name of an entity URI.</li>
 * </ul>
 */
public enum SummaryContext {

  DATASET, SINDICE

}
