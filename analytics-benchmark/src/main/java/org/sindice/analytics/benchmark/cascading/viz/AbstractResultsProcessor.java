/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;

/**
 * Base class for {@link ResultsProcessor} implementations.
 */
public abstract class AbstractResultsProcessor
implements ResultsProcessor {

  /** Round the {@link BigDecimal} value */
  private final DecimalFormat format = new DecimalFormat(BenchmarkResults.ROUND);

  /** The {@link BenchmarkResults} to store the processed benchmark results */
  protected BenchmarkResults res;

  private boolean isProcessed;

  /**
   * Returns <code>true</code> is the {@link #process(ResultsIterator)} was called at least once.
   */
  protected boolean isProcessed() {
    return isProcessed;
  }

  @Override
  public void reset() {
    res = new BenchmarkResults();
    isProcessed = false;
  }

  @Override
  public void setDataset(String dataset) {
    res.setDataset(dataset);
  }

  @Override
  public void setAlgorithm(String algorithm) {
    res.setAlgorithm(algorithm);
  }

  @Override
  public void process(ResultsIterator resultsIt)
  throws IOException {
    if (resultsIt == null) {
      throw new NullPointerException();
    }
    isProcessed = true;
  }

  @Override
  public BenchmarkResults getProcessedResults() {
    if (!isProcessed()) {
      for (String label : getProcessLabels()) {
        res.getResults().put(label, "-");
      }
    }
    return res;
  }

  /**
   * Round the given value to {@link BenchmarkResults#ROUND} and returns a string representation.
   * Returns "NaN" if the double value is {@link Double#isInfinite() infinite} or is {@link Double#isNaN() NaN}.
   * @param d the double value to round
   */
  protected String roundToString(final double d) {
    if (Double.isInfinite(d) || Double.isNaN(d)) {
      return "NaN";
    }
    return format.format(new BigDecimal(d));
  }

  /**
   * Returns the list of labels this {@link ResultsProcessor} outputs.
   */
  protected abstract String[] getProcessLabels();

}
