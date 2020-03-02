/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz.schema;

import org.sindice.analytics.benchmark.cascading.precision.schema.SchemaAssembly;
import org.sindice.analytics.benchmark.cascading.precision.schema.SchemaAssemblyCLI;
import org.sindice.analytics.benchmark.cascading.viz.ResultsIterator;
import org.sindice.core.analytics.cascading.annotation.Analytics;

import cascading.tuple.Fields;

/**
 * This {@link ResultsIterator} iterates over the raw data returned by the {@link SchemaAssembly} assembly.
 */
public class SchemaResultsIterator
extends ResultsIterator {

  @Override
  public Fields getInputFields() {
    return Analytics.getTailFields(SchemaAssembly.class);
  }

  @Override
  public Fields getOutputFields() {
    return Analytics.getTailFields(SchemaAssembly.class);
  }

  @Override
  public String getResultsPattern() {
    return SchemaAssemblyCLI.RESULTS;
  }

}
