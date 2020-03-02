/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz.connectivity;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.sindice.analytics.benchmark.cascading.viz.AbstractResultsProcessor;
import org.sindice.analytics.benchmark.cascading.viz.ResultsIterator;
import org.sindice.analytics.benchmark.cascading.viz.ResultsProcessor;

import cascading.tuple.TupleEntry;

/**
 * This {@link ResultsProcessor} computes the average connectivity precision per evaluated cluster.
 */
public class ConnectivityP1Processor
extends AbstractResultsProcessor {

  /**
   * The precision P1 computes the average precision of a node.
   * @param resultsIt the {@link ResultsIterator} for iterating over the data
   * @throws IOException if an error occurs while reading or writing results
   */
  @Override
  public void process(ResultsIterator resultsIt)
  throws IOException {
    double avg = 0;

    super.process(resultsIt);
    try {
      final Set<BytesWritable> cidsEval = new HashSet<BytesWritable>();
      while (resultsIt.hasNext()) {
        final TupleEntry te = resultsIt.next();
        cidsEval.add((BytesWritable) te.getObject("cid-eval"));
        final double truePos = te.getDouble("true");
        final double falsePos = te.getDouble("false");
        final double sizeEval = te.getDouble("size-eval");
        avg += truePos / ((truePos + falsePos) * sizeEval);
      }
      /*
       * The size of cidsEval is the number of nodes in the evaluated summary that are neither
       * <ul>
       * <li>a sink node</li>
       * <li>nor an island, i.e., a node without incoming or outgoing edges.</li>
       * </ul>
       */
      res.getResults().put("p1", roundToString(avg / cidsEval.size()));
    } finally {
      resultsIt.close();
    }
  }

  @Override
  protected String[] getProcessLabels() {
    return new String[] { "p1" };
  }

}
