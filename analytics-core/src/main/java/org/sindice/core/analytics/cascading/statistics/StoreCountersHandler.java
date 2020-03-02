/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.statistics;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.sindice.core.analytics.cascading.riffle.ProcessFlowStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import cascading.cascade.Cascade;
import cascading.flow.Flow;
import cascading.flow.FlowStep;
import cascading.management.UnitOfWork;
import cascading.stats.CascadingStats;
import cascading.stats.FlowStats;
import cascading.stats.FlowStepStats;
import cascading.stats.hadoop.HadoopSliceStats;
import cascading.stats.hadoop.HadoopStepStats;

/**
 * Stores the statistics of a {@link UnitOfWork} into YAML-formated files.
 */
public class StoreCountersHandler
implements CountersHandler {

  private final Logger       logger      = LoggerFactory.getLogger(StoreCountersHandler.class);
  private final Yaml         yaml        = new Yaml();

  /** The filename of the YAML file with global statistics about the {@link UnitOfWork} */
  public final static String GLOBAL_NAME = "global.yaml";
  /**
   * The filename of the YAML file with statistics of each Hadoop {@link FlowStep}
   * that composes a {@link UnitOfWork}.
   */
  public final static String TASKS_NAME  = "tasks.yaml";

  @Override
  public void process(final UnitOfWork<? extends CascadingStats> work,
                      final File path)
  throws IOException {
    doProcess(work.getStats(), work.getName(), path);
  }

  private void doProcess(final CascadingStats stats, final String name, final File path)
  throws IOException {
    final Collection<? extends CascadingStats> col = stats.getChildren();
    final Iterator<? extends CascadingStats> it = col.iterator();

    final File workDir = new File(path, name.replace('/', '_'));
    workDir.mkdirs();

    logger.info("Saving Counters results of the work: {}", name);
    storeGlobalCounters(stats, workDir);

    while (it.hasNext()) {
      final CascadingStats childStat = it.next();

      final File childDir = new File(workDir, childStat.getName().replace('/', '_'));
      childDir.mkdirs();

      // Store the counters
      processCascadingStats(childStat, childDir);
    }
  }

  /**
   * Store the counters values of a {@link Cascade} or a {@link Flow}.
   * @param childStat the {@link CascadingStats} to get the stats from
   * @param childDir the {@link File} where to store the values
   * @throws IOException if a problem occurs while storing the values
   */
  private void storeGlobalCounters(final CascadingStats childStat,
                                   final File childDir)
  throws IOException {
    final BufferedWriter globalResults = new BufferedWriter(new FileWriter(
      new File(childDir, GLOBAL_NAME)));
    try {
      final Map<String, Object> map = new HashMap<String, Object>();
      for (String counterGroup : childStat.getCounterGroups()) {
        final Map<String, Long> insideMap = new HashMap<String, Long>();
        for (String counter : childStat.getCountersFor(counterGroup)) {
          insideMap.put(counter, childStat
          .getCounterValue(counterGroup, counter));
        }
        map.put(counterGroup, insideMap);
      }
      map.put(ID_FIELD, childStat.getName());
      yaml.dump(map, globalResults);
    }
    finally {
      globalResults.close();
    }
  }

  /**
   * Store the counters values of a {@link Cascade} or a {@link Flow}
   * at the Task granularity.
   * @param hss the {@link HadoopStepStats} to get the stats from
   * @param dir the {@link File} where to store the values
   * @throws IOException if a problem occurs while storing the values
   */
  private void storeTasksCounters(final HadoopStepStats hss,
                                  final File dir)
  throws IOException {
    hss.captureDetail();
    final Map<String, HadoopSliceStats> mapTasks = hss.getTaskStats();
    final BufferedWriter tasksResults = new BufferedWriter(new FileWriter(
      new File(dir, TASKS_NAME)));
    try {
      if (!mapTasks.isEmpty()) {
        for (HadoopSliceStats sliceStats : mapTasks.values()) {
          tasksResults.append("---\n").append(ID_FIELD + ": " +
                                              hss.getName() +
                                              "<" +
                                              sliceStats.getTaskID()
                                               + ">\n");
          yaml.dump(sliceStats.getCounters(), tasksResults);
        }
        tasksResults.append("...\n");
      }

    } catch (IOException e) {
      logger.error("Yaml dumping has failed", e);
    }
    finally {
      tasksResults.close();
    }
  }

  private void processCascadingStats(final CascadingStats stats,
                                     final File dir)
  throws IOException {
    if (stats instanceof ProcessFlowStats) {
      // Empty string for name since the folder has already been created for it
      doProcess(stats, "", dir);
    } else if (stats instanceof HadoopStepStats) {
      // Global stats
      storeGlobalCounters(stats, dir);
      // Tasks stats
      storeTasksCounters((HadoopStepStats) stats, dir);
    } else if (stats instanceof FlowStats) {
      if (((FlowStats) stats).getStepsCount() != 1) {
        storeGlobalCounters(stats, dir);
      }
      for (FlowStepStats hss: ((FlowStats) stats).getFlowStepStats()) {
        final File d;
        if (((FlowStats) stats).getStepsCount() != 1) {
          d = new File(dir, stats.getName().replace('/', '_')
            + hss.getName().replace('/', '_'));
          d.mkdirs();
        } else {
          d = dir;
        }
        processCascadingStats(hss, d);
      }
    }
  }

}
