/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.riffle;

import org.junit.Test;
import org.sindice.core.analytics.testHelper.AbstractAnalyticsTestCase;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.StepCounters;
import cascading.stats.CascadingStats;

public class TestRiffleProcessFlow extends AbstractAnalyticsTestCase {

  @Test
  public void testFlow() throws Exception {
    String incoming = "./src/test/resources/testRiffleProcessFlow/testFlow/input.txt";
    ProcessStatsTest processStats = new ProcessStatsTest(incoming, testOutput.getAbsolutePath());
    RiffleProcessFlow<ProcessStatsTest> flow = 
        new RiffleProcessFlow<ProcessStatsTest>("RiffleProcessFlow", processStats);
    flow.complete();

    final CascadingStats cStats = flow.getStats();
    assertEquals(12, cStats.getCounterValue(ProcessStatsTest.GROUP_TEST, ProcessStatsTest.COUNTER_TEST));
    assertEquals(12, cStats.getCounterValue(StepCounters.Tuples_Read));
    assertEquals(12, cStats.getCounterValue(StepCounters.Tuples_Written));
  }

  @Test
  public void testCascade()
  throws Exception {
    String input1 = "./src/test/resources/testRiffleProcessFlow/testCascade/input1.txt";
    String input2 = "./src/test/resources/testRiffleProcessFlow/testCascade/input2.txt";

    ProcessStatsTest proc1 = new ProcessStatsTest(input1, testOutput.getAbsolutePath());
    RiffleProcessFlow<ProcessStatsTest> flow1 = new RiffleProcessFlow<ProcessStatsTest>("p1", proc1);
    ProcessStatsTest proc2 = new ProcessStatsTest(input2, testOutput.getAbsolutePath());
    RiffleProcessFlow<ProcessStatsTest> flow2 = new RiffleProcessFlow<ProcessStatsTest>("p2", proc2);

    final CascadeConnector cc = new CascadeConnector();
    final Cascade cascade = cc.connect(flow1, flow2);
    cascade.complete();

    final CascadingStats cStats = cascade.getStats();
    assertEquals(16, cStats.getCounterValue(ProcessStatsTest.GROUP_TEST, ProcessStatsTest.COUNTER_TEST));
    assertEquals(16, cStats.getCounterValue(StepCounters.Tuples_Read));
    assertEquals(16, cStats.getCounterValue(StepCounters.Tuples_Written));
  }

}
