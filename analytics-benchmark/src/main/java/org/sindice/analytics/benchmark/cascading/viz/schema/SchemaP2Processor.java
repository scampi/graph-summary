/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz.schema;

import java.io.IOException;

import org.sindice.analytics.benchmark.cascading.viz.AbstractResultsProcessor;
import org.sindice.analytics.benchmark.cascading.viz.ResultsIterator;
import org.sindice.analytics.benchmark.cascading.viz.ResultsProcessor;

import cascading.tuple.TupleEntry;

/**
 * This {@link ResultsProcessor} computes the overall schema precision.
 */
public class SchemaP2Processor
extends AbstractResultsProcessor {

  /**
   * The precision P2 computes the overall precision of a summary.
   * @param resultsIt the {@link ResultsIterator} for iterating over the data
   * @throws IOException if an error occurs while reading or writing results
   */
  public void process(ResultsIterator resultsIt)
  throws IOException {
    double typesTruePos = 0;
    double typesFalsePos = 0;

    double propsTruePos = 0;
    double propsFalsePos = 0;

    super.process(resultsIt);
    try {
      while (resultsIt.hasNext()) {
        final TupleEntry te = resultsIt.next();
        propsTruePos += te.getLong("properties-true");
        propsFalsePos += te.getLong("properties-false");

        typesTruePos += te.getLong("types-true");
        typesFalsePos += te.getLong("types-false");
      }

      res.getResults().put("type - p2", roundToString(typesTruePos / (typesTruePos + typesFalsePos)));
      res.getResults().put("attribute - p2", roundToString(propsTruePos / (propsTruePos + propsFalsePos)));
    } finally {
      resultsIt.close();
    }
  }

  @Override
  protected String[] getProcessLabels() {
    return new String[] { "type - p2", "attribute - p2" };
  }

}
