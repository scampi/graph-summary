/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz.schema;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.sindice.analytics.benchmark.cascading.viz.AbstractResultsProcessor;
import org.sindice.analytics.benchmark.cascading.viz.ResultsIterator;
import org.sindice.analytics.benchmark.cascading.viz.ResultsProcessor;

import cascading.tuple.TupleEntry;

/**
 * This {@link ResultsProcessor} computes the average schema precision per evaluated cluster.
 */
public class SchemaP1Processor
extends AbstractResultsProcessor {

  /**
   * This class contains the data for computing the precision average of type and attribute.
   */
  private class AvgTypeAttribute {
    final Set<BytesWritable> goldSet = new HashSet<BytesWritable>();
    double avgProps = 0d;
    double avgTypes = 0d;
    @Override
    public String toString() {
      return "gold-set=" + goldSet + " avg-types=" + avgTypes + " avg-props=" + avgProps;
    }
  }

  /**
   * The precision P1 computes the average precision of a node.
   * @param resultsIt the {@link ResultsIterator} for iterating over the data
   * @throws IOException if an error occurs while reading or writing results
   */
  public void process(ResultsIterator resultsIt)
  throws IOException {
    double avgProps = 0d;
    final Set<BytesWritable> withProps = new HashSet<BytesWritable>();
    double avgTypes = 0d;
    final Set<BytesWritable> withTypes = new HashSet<BytesWritable>();
    final Map<BytesWritable, AvgTypeAttribute> map = new HashMap<BytesWritable, AvgTypeAttribute>();

    super.process(resultsIt);
    try {
      while (resultsIt.hasNext()) {
        final TupleEntry te = resultsIt.next();
        final BytesWritable cidGold = (BytesWritable) te.getObject("cid-gold");
        final BytesWritable cidEval = (BytesWritable) te.getObject("cid-eval");

        if (!map.containsKey(cidEval)) {
          map.put(cidEval, new AvgTypeAttribute());
        }
        final AvgTypeAttribute entry = map.get(cidEval);
        entry.goldSet.add(cidGold);

        final double propsTruePos = te.getDouble("properties-true");
        final double propsFalsePos = te.getDouble("properties-false");
        if (propsTruePos != 0 || propsFalsePos != 0) {
          withProps.add(cidEval);
          entry.avgProps += propsTruePos / (propsTruePos + propsFalsePos);
        }

        final double typesTruePos = te.getDouble("types-true");
        final double typesFalsePos = te.getDouble("types-false");
        if (typesTruePos != 0 || typesFalsePos != 0) {
          withTypes.add(cidEval);
          entry.avgTypes += typesTruePos / (typesTruePos + typesFalsePos);
        }
      }

      for (AvgTypeAttribute r : map.values()) {
        final double sizeEval = r.goldSet.size();
        avgProps += r.avgProps / sizeEval;
        avgTypes += r.avgTypes / sizeEval;
      }

      res.getResults().put("type - p1", roundToString(avgTypes / withTypes.size()));
      res.getResults().put("attribute - p1", roundToString(avgProps / withProps.size()));
    } finally {
      resultsIt.close();
    }
  }

  @Override
  protected String[] getProcessLabels() {
    return new String[] { "type - p1", "attribute - p1" };
  }

}
