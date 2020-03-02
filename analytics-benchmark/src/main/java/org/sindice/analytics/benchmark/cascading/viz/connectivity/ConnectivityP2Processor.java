/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz.connectivity;

import java.io.IOException;

import org.sindice.analytics.benchmark.cascading.viz.AbstractResultsProcessor;
import org.sindice.analytics.benchmark.cascading.viz.ResultsIterator;
import org.sindice.analytics.benchmark.cascading.viz.ResultsProcessor;

import cascading.tuple.TupleEntry;

/**
 * This {@link ResultsProcessor} computes the overall connectivity precision.
 */
public class ConnectivityP2Processor
extends AbstractResultsProcessor {

  /**
   * The precision P2 computes the overall precision of a summary.
   * @param resultsIt the {@link ResultsIterator} for iterating over the data
   * @throws Exception if an error occurs while reading or writing results
   */
  @Override
  public void process(ResultsIterator resultsIt)
  throws IOException {
    double truePos = 0;
    double falsePos = 0;

    super.process(resultsIt);
    try {
      while (resultsIt.hasNext()) {
        final TupleEntry te = resultsIt.next();
        truePos += te.getLong("true");
        falsePos += te.getLong("false");
      }
      res.getResults().put("p2", roundToString(truePos / (truePos + falsePos)));
    } finally {
      resultsIt.close();
    }
  }

  @Override
  protected String[] getProcessLabels() {
    return new String[] { "p2" };
  }

}
