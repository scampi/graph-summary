/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading;

import java.util.Map;
import java.util.Properties;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;
import org.sindice.graphsummary.cascading.ecluster.ClusteringFactory.ClusteringAlgorithm;

import cascading.management.UnitOfWork;
import cascading.stats.CascadingStats;
import cascading.tuple.Fields;

public class DataGraphSummaryCascadeCLI extends AbstractAnalyticsCLI {

  private static final String CLUSTER_ALGORITHM = "cluster-algorithm";

  private ClusteringAlgorithm clusteringAlgorithm;

  @Override
  protected void initializeOptionParser(final OptionParser parser) {
    parser.accepts(CLUSTER_ALGORITHM, "The way you want to cluster the different elements")
          .withOptionalArg().ofType(ClusteringAlgorithm.class).required();
  }

  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(final OptionSet options)
  throws Exception {
    // CASCADE_CONFIG
    final Map<String, Properties> cascadeProperties = cascadeConf.getFlowsConfiguration(
      DataGraphSummaryCascade.FLOWS_NAME
    );
    // CLUSTERING ALGORITHM
    clusteringAlgorithm = (ClusteringAlgorithm) options.valueOf(CLUSTER_ALGORITHM);

    // Update the properties with clustering dependent parameters
    final Map<String, String> caProps = GraphSummaryConfig.get(ClusteringAlgorithm.class.getSimpleName(),
      clusteringAlgorithm.toString());
    for (Properties props : cascadeProperties.values()) {
      props.putAll(caProps);
    }

    // Run the cascade, cascade is not instantiated within this method
    // because it kept throwing an unknown error
    return DataGraphSummaryCascade.run(input.get(0),
          getInputScheme(new Fields("value")), output.get(0),
          cascadeProperties, clusteringAlgorithm, true, counters);
  }

  public static void main(String[] args) throws Exception {
    final DataGraphSummaryCascadeCLI cli = new DataGraphSummaryCascadeCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
