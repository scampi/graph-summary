/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz.volume;

import java.io.IOException;

import org.sindice.analytics.benchmark.cascading.viz.AbstractResultsProcessor;
import org.sindice.analytics.benchmark.cascading.viz.ResultsIterator;
import org.sindice.analytics.benchmark.cascading.viz.ResultsProcessor;

import cascading.tuple.TupleEntry;

/**
 * This {@link ResultsProcessor} outputs the size and order of the summary per domain.
 */
public class VolumeProcessor
extends AbstractResultsProcessor {

  public void process(ResultsIterator resultsIt)
  throws IOException {
    super.process(resultsIt);
    try {
      while (resultsIt.hasNext()) {
        final TupleEntry te = resultsIt.next();
        res.getResults().put("order", te.getString("nb-nodes"));
        res.getResults().put("size", te.getString("count-edges"));
      }
    } finally {
      resultsIt.close();
    }
  }

  @Override
  protected String[] getProcessLabels() {
    return new String[] { "domain", "nb-nodes", "count-edges" };
  }

}
