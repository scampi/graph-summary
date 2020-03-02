/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.riffle;

import java.util.LinkedList;
import java.util.List;

import riffle.process.DependencyIncoming;
import riffle.process.DependencyOutgoing;
import riffle.process.Process;
import riffle.process.ProcessComplete;
import riffle.process.ProcessStop;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.state.Counter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.stats.FlowStats;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

/**
 * Example of a {@link Process} that uses the {@link ProcessStats} annotation.
 */
@riffle.process.Process
public class ProcessStatsTest {

  /** A custom {@link org.apache.hadoop.mapred.Counters.Counter} group name */
  public static final String    GROUP_TEST    = "GroupTest";

  /** A custom counter of the {@value #GROUP_TEST} group */
  public static final String    COUNTER_TEST  = "CounterTest";

  private final String          incoming;
  private final String          outgoing;

  private final List<FlowStats> flowStatsList = new LinkedList<FlowStats>(); // maintain order

  public ProcessStatsTest(String incoming, String outgoing) {
    this.incoming = incoming;
    this.outgoing = outgoing;
  }

  @ProcessComplete
  public void complete(){
    for(int i = 0; i < 2; i++) {
      final Pipe pipe = new Each("Pipe-" + i, new Counter(GROUP_TEST, COUNTER_TEST));

      final Tap source = new Hfs(new TextDelimited(false, "\t"), incoming);
      final Tap sink = new Hfs(new TextDelimited(false, "\t"), outgoing + "_" + i, SinkMode.REPLACE);
      FlowConnector fc = new HadoopFlowConnector();
      Flow<?> flow = fc.connect(source, sink, pipe);
      flowStatsList.add(flow.getStats());
      flow.complete();
    }
  }

  @DependencyOutgoing
  public String getOutgoing()
  {
    return outgoing;
  }

  @DependencyIncoming
  public String getIncoming()
  {
    return incoming;
  }

  @ProcessStats
  public List<FlowStats> getStats() {
    return flowStatsList;
  }

  @ProcessStop
  public void stop(){

  }
}