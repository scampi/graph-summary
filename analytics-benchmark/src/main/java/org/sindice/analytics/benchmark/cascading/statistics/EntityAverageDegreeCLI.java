/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.statistics;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;
import org.sindice.core.analytics.cascading.CascadeConfYAMLoader;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.graphsummary.cascading.entity.GetClusterGraph;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.management.UnitOfWork;
import cascading.property.AppProps;
import cascading.scheme.hadoop.SequenceFile;
import cascading.stats.CascadingStats;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

public class EntityAverageDegreeCLI extends AbstractAnalyticsCLI {

  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(final OptionSet options)
  throws Exception {
    final Properties properties = cascadeConf.getFlowsConfiguration().get(CascadeConfYAMLoader.DEFAULT_PARAMETERS);

    final FlowConnector connector = new HadoopFlowConnector(properties);
    AppProps.setApplicationJarClass(properties, EntityAverageDegree.class);
    final EntityAverageDegree avgDegree = new EntityAverageDegree();

    final Tap source = new Hfs(getInputScheme(Analytics.getTailFields(GetClusterGraph.class)), input.get(0));
    final Tap avgInDeg = new Hfs(new SequenceFile(avgDegree.tails.get("avg-indegree")),
      output.get(0) + "/avg-indegree", SinkMode.REPLACE);
    final Tap avgOutDeg = new Hfs(new SequenceFile(avgDegree.tails.get("avg-outdegree")),
      output.get(0) + "/avg-outdegree", SinkMode.REPLACE);
    final Map<String, Tap> sinks = new HashMap<String, Tap>();
    sinks.put("avg-indegree", avgInDeg);
    sinks.put("avg-outdegree", avgOutDeg);

    final Flow flow = connector.connect(avgDegree.name, source, sinks, avgDegree);
    flow.complete();
    return flow;
  }

  public static void main(String[] args) throws Exception {
    final EntityAverageDegreeCLI cli = new EntityAverageDegreeCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
