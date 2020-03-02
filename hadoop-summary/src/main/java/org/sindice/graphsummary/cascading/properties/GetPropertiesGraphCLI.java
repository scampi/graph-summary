/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.properties;

import java.util.Arrays;
import java.util.Properties;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;
import org.sindice.core.analytics.cascading.CascadeConfYAMLoader;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.graphsummary.cascading.properties.GetPropertiesGraph.PropertiesProcessing;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.management.UnitOfWork;
import cascading.property.AppProps;
import cascading.scheme.hadoop.SequenceFile;
import cascading.stats.CascadingStats;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

public class GetPropertiesGraphCLI extends AbstractAnalyticsCLI {

  private final String PROCESSING = "processing";

  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(final OptionSet options)
  throws Exception {
    final Properties properties = cascadeConf.getFlowsConfiguration()
    .get(CascadeConfYAMLoader.DEFAULT_PARAMETERS);

    AppProps.setApplicationJarClass(properties, GetPropertiesGraph.class);

    final Tap source = new Hfs(getInputScheme(Analytics.getHeadFields(GetPropertiesGraph.class)), input.get(0));
    final Tap sink = new Hfs(new SequenceFile(Analytics.getTailFields(GetPropertiesGraph.class)), output.get(0));

    final GetPropertiesGraph propertiesGraph = new GetPropertiesGraph((PropertiesProcessing) options.valueOf(PROCESSING));
    final Flow flow = new HadoopFlowConnector(properties).connect(
      Analytics.getName(GetPropertiesGraph.class), source, sink, propertiesGraph);
    flow.complete();
    return flow;
  }

  @Override
  protected void initializeOptionParser(OptionParser parser) {
    parser.accepts(PROCESSING, "The processing to apply for the GetPropertiesGraph flow: " + Arrays.toString(PropertiesProcessing.values()))
          .withRequiredArg().ofType(PropertiesProcessing.class).defaultsTo(PropertiesProcessing.DEFAULT);
  }

  public static void main(final String[] args) throws Exception {
    final GetPropertiesGraphCLI cli = new GetPropertiesGraphCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
