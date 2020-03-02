/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.Set;

/**
 * Exports sets of {@link BenchmarkResults} according to a {@link FormatterType}.
 */
public interface Formatter {

  /**
   * The type of the formatter which defines how the results are displayed.
   */
  public enum FormatterType {
    /** Create a Latex table for the benchmark results */
    LATEX,
    /** Create a HTML table for the benchmark results */
    HTML
  }

  /**
   * Mark the start the format process
   * @param out the {@link Writer} to write to
   * @throws IOException if a problem appears when writing to <code>out</code>
   */
  public void start(final Writer out) throws IOException;

  /**
   * Set the name of the algorithms which results are to be exported.
   * @param out the {@link Writer} to write to
   * @param algorithms a {@link Set} of algorithms' name
   * @throws IOException if a problem appears when writing to <code>out</code>
   */
  public void setAlgorithms(final Writer out, final Set<String> algorithms) throws IOException;

  /**
   * Format a benchmark result
   * @param out the {@link Writer} to write to
   * @param brList a list of {@link BenchmarkResults results}
   * @throws IOException if a problem appears when writing to <code>out</code>
   */
  public void addBenchmarkResults(final Writer out, List<BenchmarkResults> brList) throws IOException;

  /**
   * Mark the end of the format process
   * @param out the {@link Writer} to write to
   * @throws IOException if a problem appears when writing to <code>out</code>
   */
  public void end(final Writer out) throws IOException;

}
