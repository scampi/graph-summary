/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.entity;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.graphsummary.cascading.entity.GetClusterGraph.PreProcessing;

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
public class GetClusterGraphCLI extends AbstractAnalyticsCLI {

  private final String PRE_PROCESSING = "pre-processing";

  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(final OptionSet options)
  throws Exception {
    final Map<String, Properties> properties = cascadeConf.getFlowsConfiguration(
      Analytics.getName(GetClusterGraph.class)
    );

    final FlowConnector fc = new HadoopFlowConnector(properties.get(Analytics.getName(GetClusterGraph.class)));

    final Tap source = new Hfs(getInputScheme(Analytics.getHeadFields(GetClusterGraph.class)), input.get(0));
    final Tap sink = new Hfs(new SequenceFile(Analytics.getTailFields(GetClusterGraph.class)), output.get(0), SinkMode.REPLACE);
    final Flow flow = fc.connect(Analytics.getName(GetClusterGraph.class),
      source, sink, new GetClusterGraph((PreProcessing) options.valueOf(PRE_PROCESSING)));

    flow.complete();
    return flow;
  }

  @Override
  protected void initializeOptionParser(OptionParser parser) {
    parser.accepts(PRE_PROCESSING, "The entity description pre-processing to apply: " + Arrays.toString(PreProcessing.values()))
          .withRequiredArg().ofType(PreProcessing.class).defaultsTo(PreProcessing.O_EDGES_AGGREGATE);
  }

  public static void main(String[] args) throws Exception {
    final GetClusterGraphCLI cli = new GetClusterGraphCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
