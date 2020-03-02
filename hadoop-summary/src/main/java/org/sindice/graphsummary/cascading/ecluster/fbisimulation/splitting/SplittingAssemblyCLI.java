/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster.fbisimulation.splitting;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;
import org.sindice.core.analytics.cascading.CascadeConfYAMLoader;
import org.sindice.core.analytics.cascading.annotation.Analytics;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.management.UnitOfWork;
import cascading.property.AppProps;
import cascading.scheme.hadoop.SequenceFile;
import cascading.stats.CascadingStats;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class SplittingAssemblyCLI extends AbstractAnalyticsCLI {

  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(final OptionSet options)
  throws Exception {
    final Properties properties = cascadeConf.getFlowsConfiguration().get(CascadeConfYAMLoader.DEFAULT_PARAMETERS);
    AppProps.setApplicationJarClass(properties, SplittingAssembly.class);

    final Map<String, Fields> heads = Analytics.getTailsFields(SplittingAssembly.class);
    final Tap partitionOld = new Hfs(new SequenceFile(heads.get("partition-old")), input.get(0));
    final Tap splitsOld = new Hfs(new SequenceFile(heads.get("splits-old")), input.get(1));
    final Map<String, Tap> sources = new HashMap<String, Tap>();
    sources.put("partition-old", partitionOld);
    sources.put("splits-old", splitsOld);

    final Map<String, Fields> tails = Analytics.getTailsFields(SplittingAssembly.class);
    final Tap partitionNew = new Hfs(new SequenceFile(tails.get("partition-new")), output.get(0));
    final Tap splitsNew = new Hfs(new SequenceFile(tails.get("splits-new")), output.get(1));
    final Map<String, Tap> sinks = new HashMap<String, Tap>();
    sinks.put("partition-new", partitionNew);
    sinks.put("splits-new", splitsNew);
    final SplittingAssembly s = new SplittingAssembly();
    final Flow flow = new HadoopFlowConnector(properties).connect(Analytics.getName(s), sources, sinks, s);
    flow.complete();
    return flow;
  }

  public static void main(String[] args) throws Exception {
    final SplittingAssemblyCLI cli = new SplittingAssemblyCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
