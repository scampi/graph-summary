/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.statistics;

import java.util.Properties;

import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;
import org.sindice.core.analytics.cascading.CascadeConfYAMLoader;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.graphsummary.cascading.properties.GetPropertiesGraph;

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

public class SummaryVolumeCLI extends AbstractAnalyticsCLI {

  /** The name of the {@link SummaryVolume} output */
  public static final String VOLUME_OUT = "volume";

  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(final OptionSet options)
  throws Exception {
    final Properties properties = cascadeConf.getFlowsConfiguration().get(CascadeConfYAMLoader.DEFAULT_PARAMETERS);

    final FlowConnector connector = new HadoopFlowConnector(properties);
    AppProps.setApplicationJarClass(properties, SummaryVolume.class);
    final SummaryVolume so = new SummaryVolume();

    final Tap source = new Hfs(getInputScheme(Analytics.getTailFields(GetPropertiesGraph.class)), input.get(0));
    final Tap sink = new Hfs(new SequenceFile(Analytics.getTailFields(so)), output.get(0) + "/" + VOLUME_OUT, SinkMode.REPLACE);

    final Flow flow = connector.connect(so.name, source, sink, so);
    flow.complete();
    return flow;
  }

  public static void main(String[] args) throws Exception {
    final SummaryVolumeCLI cli = new SummaryVolumeCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
