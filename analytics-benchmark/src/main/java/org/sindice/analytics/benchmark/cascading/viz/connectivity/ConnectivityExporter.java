/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz.connectivity;

import java.util.Arrays;
import java.util.List;

import org.sindice.analytics.benchmark.cascading.precision.connectivity.ConnectivityAssembly;
import org.sindice.analytics.benchmark.cascading.viz.AbstractResultsExporter;
import org.sindice.analytics.benchmark.cascading.viz.Formatter;
import org.sindice.analytics.benchmark.cascading.viz.ResultsIterator;
import org.sindice.analytics.benchmark.cascading.viz.ResultsProcessor;
import org.sindice.analytics.benchmark.cascading.viz.Formatter.FormatterType;

import cascading.scheme.Scheme;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tuple.Fields;

/**
 * This {@link ConnectivityResultsIterator} exports results
 * of the {@link ConnectivityAssembly connectivity precision error} evaluation.
 */
public class ConnectivityExporter
extends AbstractResultsExporter {

  @Override
  public String getName() {
    return "connectivity";
  }

  @Override
  protected Scheme getDefaultSchemeInstance(Fields fields) {
    return new SequenceFile(fields);
  }

  @Override
  public ResultsIterator getResultsIterator() {
    return new ConnectivityResultsIterator();
  }

  @Override
  public List<? extends ResultsProcessor> getDatasetProcessor() {
    return Arrays.asList(
      new ConnectivityP1Processor(),
      new ConnectivityP2Processor()
    );
  }

  @Override
  public Formatter getFormatter(FormatterType ft) {
    switch (ft) {
      case LATEX:
        return new ConnectivityLatexFormatter();
      case HTML:
        return new ConnectivityHtmlFormatter();
      default:
        throw new EnumConstantNotPresentException(FormatterType.class, ft.toString());
    }
  }

}
