/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.util;

/**
 * Counter labels used in Analytics applications.
 */
public class AnalyticsCounters {

  private AnalyticsCounters() {}

  /**
   * The group identifier of the Analytics counters.
   */
  public static final String GROUP = "Analytics";

  /**
   * The time in ms spent by an operation.
   * The name of the operation is appended to this constant.
   */
  public static final String TIME = "Time_";

  /**
   * The number of tuples read by an operation.
   * The name of the operation is appended to this constant.
   */
  public static final String TUPLES_READ = "Tuples_Read_";

  /**
   * The number of tuples written by an operation.
   * The name of the operation is appended to this constant.
   */
  public static final String TUPLES_WRITTEN = "Tuples_Written_";

  /**
   * This counter is to be used for counting errors, e.g., the number of bad triples.
   */
  public static final String ERROR = "Error_";

}
