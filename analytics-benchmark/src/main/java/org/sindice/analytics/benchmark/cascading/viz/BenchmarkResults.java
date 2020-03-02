/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz;

import java.util.Map;
import java.util.TreeMap;

/**
 * A {@link BenchmarkResults} stores the output of a {@link ResultsProcessor}.
 * The stored data can be set / retrieved via {@link #getResults()}.
 * In presence of numerical data, the value is rounded to {@value #ROUND}.
 */
public class BenchmarkResults
implements Comparable<BenchmarkResults> {

  /** Round numeric values up to the n-th precision */
  public static String              ROUND   = "0.00";
  /** The name of the dataset */
  private String                    dataset;
  /** The name of the algorithm */
  private String                    algorithm;
  /** The results of the algorithm on the dataset */
  private final Map<String, String> results = new TreeMap<String, String>();

  /**
   * Set the rounding of values to the n-th decimal
   * @param n the decimal precision
   */
  public static void setRound(int n) {
    ROUND = "0.";
    for (int i = 0; i < n; i++) {
      ROUND += "0";
    }
  }

  @Override
  public int compareTo(BenchmarkResults o) {
    int c = this.dataset.compareTo(o.dataset);
    if (c != 0) {
      return c;
    }
    c = algorithm.compareTo(o.algorithm);
    if (c != 0) {
      return c;
    }

    final String thisstr = results.toString();
    final String ostr = o.results.toString();
    return thisstr.compareTo(ostr);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof BenchmarkResults) {
      final BenchmarkResults br = (BenchmarkResults) obj;
      return br.compareTo(this) == 0;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hash = 1;
    hash = 31 * hash + dataset.hashCode();
    hash = 31 * hash + algorithm.hashCode();
    hash = 31 * hash + results.hashCode();
    return hash;
    
  }

  /**
   * @param dataset the dataset to set
   */
  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  /**
   * @param algorithm the algorithm to set
   */
  public void setAlgorithm(String algorithm) {
    this.algorithm = algorithm;
  }

  /**
   * Returns the name of the algorithm that produced the results on the dataset
   */
  public String getAlgorithm() {
    return algorithm;
  }

  /**
   * Returns the name of the dataset that provided the results
   */
  public String getDataset() {
    return dataset;
  }

  /**
   * Returns the data processed by {@link ResultsProcessor}.
   */
  public Map<String, String> getResults() {
    return results;
  }

  @Override
  public String toString() {
    return "dataset=" + dataset + " algorithm=" + algorithm + " results=" + results;
  }

}
