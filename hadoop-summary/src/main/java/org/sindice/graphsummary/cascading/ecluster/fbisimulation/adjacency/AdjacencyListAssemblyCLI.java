/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster.fbisimulation.adjacency;

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
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.management.UnitOfWork;
import cascading.property.AppProps;
import cascading.scheme.hadoop.SequenceFile;
import cascading.stats.CascadingStats;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class AdjacencyListAssemblyCLI extends AbstractAnalyticsCLI {

  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(final OptionSet options)
  throws Exception {
    final Properties properties = cascadeConf.getFlowsConfiguration().get(CascadeConfYAMLoader.DEFAULT_PARAMETERS);
    AppProps.setApplicationJarClass(properties, AdjacencyListAssembly.class);

    final Tap source = new Hfs(getInputScheme(Analytics.getTailFields(GetClusterGraph.class)), input.get(0));
    final Map<String, Fields> tails = Analytics.getTailsFields(AdjacencyListAssembly.class);
    final Tap intraSet = new Hfs(new SequenceFile(tails.get("intra")), output.get(0));
    final Tap sourcesSet = new Hfs(new SequenceFile(tails.get("sources")), output.get(1));
    final Tap sinksSet = new Hfs(new SequenceFile(tails.get("sinks")), output.get(2));
    final Map<String, Tap> sinks = new HashMap<String, Tap>();
    sinks.put("intra", intraSet);
    sinks.put("sources", sourcesSet);
    sinks.put("sinks", sinksSet);
    final AdjacencyListAssembly adj = new AdjacencyListAssembly();
    final Flow flow = new HadoopFlowConnector(properties).connect(Analytics.getName(adj), source, sinks, adj);
    flow.complete();
    return flow;
  }

  public static void main(String[] args) throws Exception {
    final AdjacencyListAssemblyCLI cli = new AdjacencyListAssemblyCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
