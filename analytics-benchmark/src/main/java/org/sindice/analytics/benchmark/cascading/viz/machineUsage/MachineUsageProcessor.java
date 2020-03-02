/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz.machineUsage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sindice.analytics.benchmark.cascading.viz.AbstractResultsProcessor;
import org.sindice.analytics.benchmark.cascading.viz.BenchmarkResults;
import org.sindice.analytics.benchmark.cascading.viz.ResultsIterator;
import org.sindice.analytics.benchmark.cascading.viz.ResultsProcessor;

import cascading.tuple.TupleEntry;

/**
 * This {@link ResultsProcessor} computes the average usage of a machine accross several runs.
 */
public class MachineUsageProcessor
extends AbstractResultsProcessor {

  /** The measurements over the runs */
  private final Map<String, List<Long>> data = new HashMap<String, List<Long>>();

  public void process(ResultsIterator resultsIt)
  throws IOException {
    super.process(resultsIt);
    try {
      while (resultsIt.hasNext()) {
        final TupleEntry te = resultsIt.next();
        addMeasurement(te, "cpu");
        addMeasurement(te, "io");
        addMeasurement(te, "memory");
      }
    } finally {
      resultsIt.close();
    }
  }

  /**
   * Add the value of a measurement to {@link #data}
   * @param te the {@link TupleEntry} with the raw value
   * @param fieldName the field name of the measurement
   */
  private void addMeasurement(final TupleEntry te, final String fieldName) {
    if (!data.containsKey(fieldName)) {
      data.put(fieldName, new ArrayList<Long>());
    }
    data.get(fieldName).add(te.getLong(fieldName));
  }

  @Override
  public void reset() {
    super.reset();
    data.clear();
  }

  @Override
  public BenchmarkResults getProcessedResults() {
    if (!isProcessed()) {
      return super.getProcessedResults();
    }

    final double cpuMean = getMean(data.get("cpu"));
    final double cpuSD = getStandardDeviation(data.get("cpu"), cpuMean);
    res.getResults().put("cpu (ms)", roundToString(cpuMean) + " +- " + roundToString(cpuSD));

    final double ioMean = getMean(data.get("io"));
    final double ioSD = getStandardDeviation(data.get("io"), ioMean);
    res.getResults().put("io (Gb)", roundToString(ioMean / (1024 * 1024 * 1024)) +
        " +- " + roundToString(ioSD / (1024 * 1024 * 1024)));

    final double memoryMean = getMean(data.get("memory"));
    final double memorySD = getStandardDeviation(data.get("memory"), memoryMean);
    res.getResults().put("memory (Mb)", roundToString(memoryMean / (1024 * 1024)) +
        " +- " + roundToString(memorySD / (1024 * 1024)));
    return res;
  }


  @Override
  protected String[] getProcessLabels() {
    return new String[] { "cpu (ms)", "io (Gb)", "memory (Mb)" };
  }

  /**
   * Returns the standard deviation for the list of longs
   * Returns {@link Double#NaN} if there was only one on no value.
   */
  private double getStandardDeviation(final List<Long> values, double mean) {
    double sd = 0;

    for (long l : values) {
      sd += Math.pow(l - mean, 2);
    }
    if (values.size() <= 1) {
      return Double.NaN;
    }
    return Math.sqrt(sd / (values.size() - 1));
  }

  /**
   * Returns the mean of the list of longs.
   * Returns {@link Double#NaN} if there was no value.
   */
  private double getMean(final List<Long> values) {
    long mean = 0;

    if (values.size() == 0) {
      return Double.NaN;
    }
    for (long l : values) {
      mean += l;
    }
    return mean / (double) values.size();
  }

}
