/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz.volume;

import java.util.Arrays;
import java.util.List;

import org.sindice.analytics.benchmark.cascading.viz.AbstractResultsExporter;
import org.sindice.analytics.benchmark.cascading.viz.Formatter;
import org.sindice.analytics.benchmark.cascading.viz.Formatter.FormatterType;
import org.sindice.analytics.benchmark.cascading.viz.ResultsExporter;
import org.sindice.analytics.benchmark.cascading.viz.ResultsIterator;
import org.sindice.analytics.benchmark.cascading.viz.ResultsProcessor;

import cascading.scheme.Scheme;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tuple.Fields;

/**
 * This {@link ResultsExporter} exports the volume of a graph summary, i.e., the number of edges (size) and the number
 * of nodes (order).
 */
public class VolumeExporter
extends AbstractResultsExporter {

  @Override
  public String getName() {
    return "volume";
  }

  @Override
  protected Scheme getDefaultSchemeInstance(Fields fields) {
    return new SequenceFile(fields);
  }

  @Override
  public ResultsIterator getResultsIterator() {
    return new VolumeResultsIterator();
  }

  @Override
  public List<? extends ResultsProcessor> getDatasetProcessor() {
    return Arrays.asList(new VolumeProcessor());
  }

  @Override
  public Formatter getFormatter(FormatterType ft) {
    switch (ft) {
      case LATEX:
        return new VolumeLatexFormatter();
      case HTML:
        return new VolumeHtmlFormatter();
      default:
        throw new EnumConstantNotPresentException(FormatterType.class, ft.toString());
    }
  }

}
