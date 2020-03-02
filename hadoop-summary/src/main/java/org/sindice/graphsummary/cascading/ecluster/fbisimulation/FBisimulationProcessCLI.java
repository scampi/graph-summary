/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster.fbisimulation;

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
import cascading.flow.hadoop.ProcessFlow;
import cascading.management.UnitOfWork;
import cascading.property.AppProps;
import cascading.scheme.hadoop.SequenceFile;
import cascading.stats.CascadingStats;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

public class FBisimulationProcessCLI extends AbstractAnalyticsCLI {

  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(final OptionSet options)
  throws Exception {
    final Properties properties = cascadeConf.getFlowsConfiguration().get(CascadeConfYAMLoader.DEFAULT_PARAMETERS);
    AppProps.setApplicationJarClass(properties, FBisimulationProcess.class);

    final Tap source = new Hfs(getInputScheme(Analytics.getTailFields(GetClusterGraph.class)), input.get(0));
    final Tap sink = new Hfs(new SequenceFile(Analytics.getTailFields(ClusterSubAssembly.class)), output.get(0));
    final FBisimulationProcess ref = new FBisimulationProcess(source, sink, properties);
    final Flow flow = new ProcessFlow<FBisimulationProcess>(Analytics.getName(ClusterSubAssembly.class), ref);
    flow.complete();
    return flow;
  }

  public static void main(String[] args) throws Exception {
    final FBisimulationProcessCLI cli = new FBisimulationProcessCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
