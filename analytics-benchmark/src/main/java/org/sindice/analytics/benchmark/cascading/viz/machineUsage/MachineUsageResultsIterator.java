/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz.machineUsage;

import java.util.Map;

import org.sindice.analytics.benchmark.cascading.viz.ResultsIterator;
import org.sindice.core.analytics.cascading.statistics.StoreCountersHandler;
import org.yaml.snakeyaml.Yaml;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * This {@link ResultsIterator} iterates over the raw data saved in the YAML file {@link StoreCounters#GLOBAL_NAME}.
 */
public class MachineUsageResultsIterator
extends ResultsIterator {

  private final TupleEntry out = new TupleEntry(getOutputFields(), Tuple.size(getOutputFields().size()));
  private final Yaml yaml = new Yaml();

  @Override
  public Fields getInputFields() {
    return new Fields("yaml");
  }

  @Override
  public Fields getOutputFields() {
    return new Fields("cpu", "io", "memory");
  }

  @Override
  public TupleEntry next() {
    final TupleEntry te = super.next();
    final String data = te.getString("yaml");

    final Map<String, Object> map = (Map<String, Object>) yaml.load(data);
    final Map<String, Number> tasks = (Map<String, Number>) map.get("org.apache.hadoop.mapred.Task$Counter");
    final Map<String, Number> fs = (Map<String, Number>) map.get("FileSystemCounters");

    if (tasks == null) {
      throw new IllegalArgumentException("Missing counters related to [org.apache.hadoop.mapred.Task$Counter]");
    }
    if (fs == null) {
      throw new IllegalArgumentException("Missing counters related to [FileSystemCounters]");
    }

    final long cpu = tasks.get("CPU_MILLISECONDS").longValue();
    final long read = fs.get("FILE_BYTES_READ").longValue();
    final long written = fs.get("FILE_BYTES_WRITTEN").longValue();
    final long memory = tasks.get("VIRTUAL_MEMORY_BYTES").longValue();
    out.setLong("cpu", cpu);
    out.setLong("io", read + written);
    out.setLong("memory", memory);
    return out;
  }

  @Override
  public String getResultsPattern() {
    return "run\\d+-" + StoreCountersHandler.GLOBAL_NAME;
  }

}
