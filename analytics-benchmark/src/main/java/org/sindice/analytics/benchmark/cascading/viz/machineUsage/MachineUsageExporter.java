/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz.machineUsage;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.mapred.Task.Counter;
import org.sindice.analytics.benchmark.cascading.viz.AbstractResultsExporter;
import org.sindice.analytics.benchmark.cascading.viz.Formatter;
import org.sindice.analytics.benchmark.cascading.viz.ResultsExporter;
import org.sindice.analytics.benchmark.cascading.viz.ResultsIterator;
import org.sindice.analytics.benchmark.cascading.viz.ResultsProcessor;
import org.sindice.analytics.benchmark.cascading.viz.Formatter.FormatterType;
import org.sindice.core.analytics.cascading.scheme.WholeFile;

import cascading.scheme.Scheme;
import cascading.tuple.Fields;

/**
 * This {@link ResultsExporter} exports results about the machine usage of an algorithm on the dataset.
 * Several measures about the usage of a machine reported:
 * <ul>
 * <li><b>cpu</b>: the CPU time in ms, as reported by the Hadoop counter {@link Counter#CPU_MILLISECONDS};</li>
 * <li><b>io</b>: the amount of data read and written in Gb, as the sum of FILE_BYTES_READ and FILE_BYTES_WRITTEN;</li>
 * <li><b>memory</b>: the amount of virtual memory used in Mb, as reported by
 * the Hadoop counter {@link Counter#VIRTUAL_MEMORY_BYTES}</li>
 * </ul>
 */
public class MachineUsageExporter
extends AbstractResultsExporter {

  @Override
  public String getName() {
    return "usage";
  }

  @Override
  protected Scheme getDefaultSchemeInstance(Fields fields) {
    return new WholeFile(fields);
  }

  @Override
  public ResultsIterator getResultsIterator() {
    return new MachineUsageResultsIterator();
  }

  @Override
  public List<? extends ResultsProcessor> getDatasetProcessor() {
    return Arrays.asList(new MachineUsageProcessor());
  }

  @Override
  public Formatter getFormatter(FormatterType ft) {
    switch (ft) {
      case LATEX:
        return new MachineUsageLatexFormatter();
      case HTML:
        return new MachineUsageHtmlFormatter();
      default:
        throw new EnumConstantNotPresentException(FormatterType.class, ft.toString());
    }
  }

}
