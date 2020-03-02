/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz.schema;

import java.util.Arrays;
import java.util.List;

import org.sindice.analytics.benchmark.cascading.precision.schema.SchemaAssembly;
import org.sindice.analytics.benchmark.cascading.viz.AbstractResultsExporter;
import org.sindice.analytics.benchmark.cascading.viz.Formatter;
import org.sindice.analytics.benchmark.cascading.viz.ResultsExporter;
import org.sindice.analytics.benchmark.cascading.viz.ResultsIterator;
import org.sindice.analytics.benchmark.cascading.viz.ResultsProcessor;
import org.sindice.analytics.benchmark.cascading.viz.Formatter.FormatterType;

import cascading.scheme.Scheme;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tuple.Fields;

/**
 * This {@link ResultsExporter} exports the results about the schema error, as reported by the {@link SchemaAssembly}.
 */
public class SchemaExporter
extends AbstractResultsExporter {

  @Override
  public String getName() {
    return "schema";
  }

  @Override
  protected Scheme getDefaultSchemeInstance(Fields fields) {
    return new SequenceFile(fields);
  }

  @Override
  public ResultsIterator getResultsIterator() {
    return new SchemaResultsIterator();
  }

  @Override
  public List<? extends ResultsProcessor> getDatasetProcessor() {
    return Arrays.asList(
      new SchemaP1Processor(),
      new SchemaP2Processor()
    );
  }

  @Override
  public Formatter getFormatter(FormatterType ft) {
    switch (ft) {
      case LATEX:
        return new SchemaLatexFormatter();
      case HTML:
        return new SchemaHtmlFormatter();
      default:
        throw new EnumConstantNotPresentException(FormatterType.class, ft.toString());
    }
  }

}
