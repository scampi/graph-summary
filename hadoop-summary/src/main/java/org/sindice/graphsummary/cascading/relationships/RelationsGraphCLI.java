/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.relationships;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;
import org.sindice.core.analytics.cascading.CascadeConfYAMLoader;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.graphsummary.cascading.ecluster.ClusterSubAssembly;
import org.sindice.graphsummary.cascading.entity.GetClusterGraph;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.management.UnitOfWork;
import cascading.scheme.hadoop.SequenceFile;
import cascading.stats.CascadingStats;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

/**
 * 
 */
public class RelationsGraphCLI extends AbstractAnalyticsCLI {

  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(final OptionSet options)
  throws Exception {
    final Properties properties = cascadeConf.getFlowsConfiguration().get(CascadeConfYAMLoader.DEFAULT_PARAMETERS);

    final Map<String, Tap> sources = new HashMap<String, Tap>();
    sources.put(Analytics.getName(GetClusterGraph.class),
      new Hfs(getInputScheme(Analytics.getTailFields(GetClusterGraph.class)), input.get(0)));
    sources.put(Analytics.getName(ClusterSubAssembly.class),
      new Hfs(getInputScheme(Analytics.getTailFields(ClusterSubAssembly.class)), input.get(1)));
    final Tap sink = new Hfs(new SequenceFile(Analytics.getTailFields(RelationsGraph.class)), output.get(0), SinkMode.REPLACE);

    final FlowConnector fc = new HadoopFlowConnector(properties);
    final Flow flow = fc.connect(Analytics.getName(RelationsGraph.class), sources, sink, new RelationsGraph());

    flow.complete();
    return flow;
  }

  public static void main(String[] args) throws Exception {
    final RelationsGraphCLI cli = new RelationsGraphCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
