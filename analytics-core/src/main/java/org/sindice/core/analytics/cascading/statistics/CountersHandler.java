/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.statistics;

import java.io.File;
import java.io.IOException;

import cascading.flow.FlowStep;
import cascading.management.UnitOfWork;
import cascading.stats.CascadingStats;

/**
 * The {@link CountersHandler} interface allows to process the statistics of an {@link UnitOfWork}.
 */
public interface CountersHandler {

  /** The identifier of a {@link FlowStep} */
  public static final String ID_FIELD = "ID";

  /**
   * Process the {@link UnitOfWork} and output the results into a file in path
   * @param work the {@link UnitOfWork} to process
   * @param path the path to the output folder
   */
  public void process(UnitOfWork<? extends CascadingStats> work, File path) throws IOException;

}
