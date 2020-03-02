/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.hash2value.rdf;

import org.sindice.core.analytics.util.Hash;


public class DataGraphSummaryVocab {

  public static final String  ANY23_PREFIX          = "http://vocab.sindice.net/";
  public static final String  DGS_PREFIX            = "http://vocab.sindice.net/analytics#";
  private static final String DEFAULT_DUP           = "http://sindice.com/dataspace/default/domain/";
  public static String        DOMAIN_URI_PREFIX     = DEFAULT_DUP;

  /*
   * Analytics Graphs Names
   */
  public static final String  DEFAULT_GSG           = "http://sindice.com/analytics";
  public static String        GRAPH_SUMMARY_GRAPH   = DEFAULT_GSG;

  public static final String  DUMMY_CLASS_HASH      = Long
                                                    .toString(Hash.getHash64("dummy class"))
                                                    .replace('-', 'n');
  public static final String  BLANK_NODE_COLLECTION = "dummy class: " +
                                                      DUMMY_CLASS_HASH;

  /**
   * Vocabulary Terms
   */
  public static final String  DOMAIN_URI            = ANY23_PREFIX + "domain_uri";
  public static final String  DOMAIN_NAME           = ANY23_PREFIX + "domain";
  public static final String  LABEL                 = DGS_PREFIX + "label";
  public static final String  CARDINALITY           = DGS_PREFIX + "cardinality";
  public static final String  GLOBAL_ID             = DGS_PREFIX + "global_id";
  public static final String  TYPE                  = DGS_PREFIX + "type";
  public static final String  EDGE_SOURCE           = DGS_PREFIX + "source";
  public static final String  EDGE_TARGET           = DGS_PREFIX + "target";
  public static final String  EDGE_PUBLISHED_IN     = DGS_PREFIX + "publishedIn";
  public static final String  EDGE_DATATYPE         = DGS_PREFIX + "datatype";

  public static void setDomainUriPrefix(String dup) {
    DOMAIN_URI_PREFIX = dup;
  }

  public static void setGraphSummaryGraph(String gsg) {
    GRAPH_SUMMARY_GRAPH = gsg;
  }

  public static void resetToDefaults() {
    DOMAIN_URI_PREFIX = DEFAULT_DUP;
    GRAPH_SUMMARY_GRAPH = DEFAULT_GSG;
  }

}
