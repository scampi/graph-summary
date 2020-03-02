/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.riffle;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import cascading.flow.Flow;
import cascading.flow.hadoop.ProcessFlow;
import cascading.management.state.ClientState;
import cascading.stats.FlowStats;

/**
 * The {@link ProcessFlowStats} collects statistics specific to a {@link ProcessFlow}.
 * <p>
 * This implementation assumes that the {@link ProcessFlow} executes a series of Hadoop jobs.
 * Therefore, {@link #setFlowStatsList(List)} takes a {@link List} of {@link FlowStats}, each
 * {@link FlowStats} being the statistics of a job.
 */
public class ProcessFlowStats
extends FlowStats {

  private static final long serialVersionUID = -8378679874862912791L;
  private List<FlowStats>   flowStatsList;

  public ProcessFlowStats(Flow flow, ClientState clientState) {
    super(flow, clientState);
  }

  /**
   * Sets the {@link List} of {@link FlowStats}.
   */
  public void setFlowStatsList(List<FlowStats> flowStatsList) {
    this.flowStatsList = flowStatsList;
  }

  @Override
  public Collection<String> getCounterGroups() {
    Set<String> results = new HashSet<String>();

    for (FlowStats flowStats : flowStatsList)
      results.addAll(flowStats.getCounterGroups());

    return results;
  }

  @Override
  public Collection<String> getCounterGroupsMatching(String regex) {
    Set<String> results = new HashSet<String>();

    for (FlowStats flowStats : flowStatsList)
      results.addAll(flowStats.getCounterGroupsMatching(regex));

    return results;
  }

  @Override
  public Collection<String> getCountersFor(String group) {
    Set<String> results = new HashSet<String>();

    for (FlowStats flowStats : flowStatsList)
      results.addAll(flowStats.getCountersFor(group));

    return results;
  }

  @Override
  public long getCounterValue(Enum counter) {
    long value = 0;

    for (FlowStats flowStats : flowStatsList)
      value += flowStats.getCounterValue(counter);

    return value;
  }

  @Override
  public long getCounterValue(String group, String counter) {
    long value = 0;

    for (FlowStats flowStats : flowStatsList)
      value += flowStats.getCounterValue(group, counter);

    return value;
  }

  @Override
  public void captureDetail() {
    for (FlowStats flowStats : flowStatsList)
      flowStats.captureDetail();
  }

  @Override
  public Collection getChildren() {
    return flowStatsList;
  }

  @Override
  public String toString() {
    return "ProcessFlow{" + "flowStatsList=" + flowStatsList + '}';
  }

}
