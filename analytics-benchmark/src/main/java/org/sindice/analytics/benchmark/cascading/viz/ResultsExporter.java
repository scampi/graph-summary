/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz;

import java.util.List;

import org.sindice.analytics.benchmark.cascading.viz.Formatter.FormatterType;

import cascading.scheme.Scheme;
import cascading.tuple.Fields;

/**
 * Contains the actual implementations to export the set of results.
 * <p>
 * A new {@link ResultsExporter} should be added to the script <b>src/main/bash/export.sh</b>.
 */
public interface ResultsExporter {

  /**
   * Returns the name of this {@link ResultsExporter}
   */
  public String getName();

  /**
   * This method sets the {@link Scheme} that should be used on the input data by this {@link #getResultsIterator()}.
   */
  public void setSchemeClass(Class<? extends Scheme> schemeCLass);

  /**
   * Returns the {@link Scheme} class that should be used on the input data by this {@link #getResultsIterator()}.
   * @param fields the {@link Fields} to pass as argument to the scheme constructor.
   * @return a {@link Scheme} instance
   */
  public Scheme getSchemeInstance(final Fields fields);

  /**
   * Returns the {@link ResultsIterator} implementation which extracts the
   * desired measurements.
   */
  public ResultsIterator getResultsIterator();

  /**
   * Returns the {@link ResultsProcessor}s implementations that process
   * the collected results by {@link #getResultsIterator()}.
   */
  public List<? extends ResultsProcessor> getDatasetProcessor();

  /**
   * Returns the {@link Formatter} implementation that formats the results processed by {@link #getDatasetProcessor()}.
   */
  public Formatter getFormatter(FormatterType ft);

}
