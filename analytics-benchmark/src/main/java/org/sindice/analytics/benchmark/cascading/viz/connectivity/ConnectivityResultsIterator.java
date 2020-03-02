/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz.connectivity;

import org.sindice.analytics.benchmark.cascading.precision.connectivity.ConnectivityAssembly;
import org.sindice.analytics.benchmark.cascading.precision.connectivity.ConnectivityAssemblyCLI;
import org.sindice.analytics.benchmark.cascading.viz.ResultsIterator;
import org.sindice.core.analytics.cascading.annotation.Analytics;

import cascading.tuple.Fields;

/**
 * This {@link ResultsIterator} iterates over the raw data returned by the {@link ConnectivityAssembly} assembly.
 */
public class ConnectivityResultsIterator
extends ResultsIterator {

  @Override
  public Fields getInputFields() {
    return Analytics.getTailFields(ConnectivityAssembly.class);
  }

  @Override
  public Fields getOutputFields() {
    return Analytics.getTailFields(ConnectivityAssembly.class);
  }

  @Override
  public String getResultsPattern() {
    return ConnectivityAssemblyCLI.RESULTS;
  }

}
