/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.statistics;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import cascading.management.UnitOfWork;
import cascading.stats.CascadingStats;
import cascading.stats.FlowStats;
import cascading.stats.FlowStepStats;
import cascading.stats.hadoop.HadoopStepStats;

/**
 * This class extracts counters' values for each child of an {@link UnitOfWork}
 * (and itself) and stores them into a same file.
 */
public class StoreOverviewCountersHandler
implements CountersHandler {

  private final Logger logger = LoggerFactory.getLogger(StoreCountersHandler.class);
  private static final String FILE_NAME = "overview.yaml";

  @Override
  public void process(final UnitOfWork<? extends CascadingStats> work,
                      final File path)
  throws IOException {
    final Collection<? extends CascadingStats> col = work.getStats().getChildren();

    if (col.size() > 1) {
      // Keep the order of the flow graph
      final Map<String, Object> stepCounters = new LinkedHashMap<String, Object>();
      final Iterator<? extends CascadingStats> it = col.iterator();

      final File workDir = new File(path, work.getName().replace('/', '_'));
      workDir.mkdirs();

      logger.info("Saving Hadoop Step Counters of the work: {}", work.getName());
      final String workName;
      if (work.getName().length() > 50) { // for readability of the results
        final String wn = work.getName();
        workName = wn.substring(0, 10) + "..." + wn.substring(wn.length() - 11);
      } else {
        workName = work.getName();
      }

      while (it.hasNext()) {
        final CascadingStats childStat = it.next();

        final String childName = childStat.getName().replace('/', '_');
        processCascadingStats(childStat, stepCounters, childName);
      }

      getCounters(work.getStats(), stepCounters, workName);
      store(workDir, stepCounters);
    }
  }

  /**
   * Store the counters into a file, YAML formatted.
   */
  private void store(final File workDir,
                     final Map<String, Object> stepCounters)
  throws IOException {
    final BufferedWriter out = new BufferedWriter(new FileWriter(
      new File(workDir, FILE_NAME)));
    try {
      final Yaml yaml = new Yaml();
      final List<Object> list = new ArrayList<Object>();
      for (Entry<String, Object> chunk: stepCounters.entrySet()) {
        final Map<String, Object> map = (Map<String, Object>) chunk.getValue();
        map.put(ID_FIELD, chunk.getKey());
        list.add(map);
      }
      yaml.dumpAll(list.listIterator(), out);
    } finally {
      out.close();
    }
  }

  /**
   * Process recursively the {@link CascadingStats}, and extract counter values
   * for each step.
   */
  private void processCascadingStats(final CascadingStats stats,
                                     final Map<String, Object> stepCounters,
                                     final String childName)
  throws IOException {
    if (stats instanceof HadoopStepStats) {
      // Step stats
      getCounters(stats, stepCounters, childName);
    } else if (stats instanceof FlowStats) {
      if (((FlowStats) stats).getStepsCount() != 1) {
        getCounters(stats, stepCounters, childName);
      }
      for (FlowStepStats hss: ((FlowStats) stats).getFlowStepStats()) {
        final String stepName;
        if (((FlowStats) stats).getStepsCount() != 1) {
          final String n = stats.getName().replace('/', '_')
                           + hss.getName().replace('/', '_');
          stepName = childName + n;
        } else {
          stepName = childName;
        }
        processCascadingStats(hss, stepCounters, stepName);
      }
    }
  }

  /**
   * Retrieve the counters values from the {@link CascadingStats}.
   */
  private void getCounters(final CascadingStats childStat,
                           final Map<String, Object> counters,
                           final String childName)
  throws IOException {
    for (String group : childStat.getCounterGroups()) {
      final Map<String, Long> insideMap = new TreeMap<String, Long>();
      for (String counter : childStat.getCountersFor(group)) {
        insideMap.put(counter, childStat.getCounterValue(group, counter));
      }
      if (!counters.containsKey(childName)) {
        counters.put(childName, new TreeMap<String, Object>());
      }
      ((Map<String, Object>) counters.get(childName)).put(group, insideMap);
    }
  }

}
