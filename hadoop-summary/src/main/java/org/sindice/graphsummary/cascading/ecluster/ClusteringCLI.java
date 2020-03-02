/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster;

import java.util.Map;
import java.util.Properties;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;
import org.sindice.core.analytics.cascading.CascadeConfYAMLoader;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.graphsummary.cascading.GraphSummaryConfig;
import org.sindice.graphsummary.cascading.ecluster.ClusteringFactory.ClusteringAlgorithm;

import cascading.flow.Flow;
import cascading.management.UnitOfWork;
import cascading.scheme.hadoop.SequenceFile;
import cascading.stats.CascadingStats;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

public class ClusteringCLI extends AbstractAnalyticsCLI {

  private static final String ALGORITHM = "algorithm";

  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(final OptionSet options)
  throws Exception {
    final Properties properties = cascadeConf.getFlowsConfiguration().get(CascadeConfYAMLoader.DEFAULT_PARAMETERS);
    final ClusteringAlgorithm algo = (ClusteringAlgorithm) options.valueOf(ALGORITHM);

    // Update the properties with clustering dependent parameters
    final Map<String, String> caProps = GraphSummaryConfig.get(ClusteringAlgorithm.class.getSimpleName(), algo.toString());
    properties.putAll(caProps);

    final Tap source = new Hfs(getInputScheme(Analytics.getHeadFields(ClusterSubAssembly.class)), input.get(0));
    final Tap sink = new Hfs(new SequenceFile(Analytics.getTailFields(ClusterSubAssembly.class)), output.get(0));

    final Flow flow = ClusteringFactory.launchAlgorithm(algo, properties, source, sink);
    flow.complete();

    return flow;
  }

  @Override
  protected void initializeOptionParser(OptionParser parser) {
    parser.accepts(ALGORITHM, "The name of the algorithm to execute")
          .withRequiredArg().ofType(ClusteringAlgorithm.class);
  }

  public static void main(final String[] args) throws Exception {
    final ClusteringCLI cli = new ClusteringCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
