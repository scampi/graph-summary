/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz;

import java.io.IOException;

/**
 * A {@link ResultsProcessor} apply some post-processing operation on the benchmark results.
 */
public interface ResultsProcessor {

  /**
   * Reset this processor so that new data can be processed.
   */
  public void reset();

  /**
   * Set the dataset name of the benchmark results to be processed
   */
  public void setDataset(String dataset);

  /**
   * Set the algorithm name of the benchmark results to be processed
   */
  public void setAlgorithm(String algorithm);

  /**
   * Process the data returned by this {@link ResultsIterator resultsIt}.
   * @param resultsIt the {@link ResultsIterator} for iterating over the raw data.
   * @throws IOException if an error occurred while iterating over the data
   */
  public void process(ResultsIterator resultsIt) throws IOException;

  /**
   * Returns the processed benchmark results.
   */
  public BenchmarkResults getProcessedResults();

}
