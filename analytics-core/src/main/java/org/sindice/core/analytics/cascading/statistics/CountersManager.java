/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.statistics;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cascading.management.UnitOfWork;
import cascading.stats.CascadingStats;

/**
 * The {@link CountersManager} applies a set of {@link CountersHandler} over a {@link UnitOfWork}.
 */
public class CountersManager {

  private List<CountersHandler> operations = new ArrayList<CountersHandler>();

  public CountersManager() {
  }

  /**
   * Executes the list of {@link CountersHandler} operations over the given {@link UnitOfWork}, saving relevant
   * information into the folder located at path.
   * @param work the {@link UnitOfWork} to process
   * @param path the path to the output folder
   */
  public void doPipeline(UnitOfWork<? extends CascadingStats> work, File path)
  throws IOException {
    if (operations.isEmpty()) {
      addDefaults();
    }
    for (CountersHandler handler : operations) {
      handler.process(work, path);
    }
  }

  /**
   * Adds a {@link CountersHandler} to the list of {@link CountersHandler} operations.
   * @param handler
   */
  public void add(CountersHandler handler) {
    operations.add(handler);
  }

  /**
   * The default set of {@link CountersHandler} operations.
   */
  private void addDefaults() {
    operations.add(new StoreCountersHandler());
    operations.add(new StoreOverviewCountersHandler());
    operations.add(new LatexCountersExportHandler());
    operations.add(new UsageCountersHandler());
  }

}
