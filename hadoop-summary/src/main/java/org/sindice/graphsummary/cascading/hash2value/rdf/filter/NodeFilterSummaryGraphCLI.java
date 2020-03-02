/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.hash2value.rdf.filter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import joptsimple.OptionParser;
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
import cascading.scheme.hadoop.TextLine;
import cascading.stats.CascadingStats;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class NodeFilterSummaryGraphCLI extends AbstractAnalyticsCLI {

  public final static String FILTER_QUERY = "filter-query";

  @Override
  protected void initializeOptionParser(OptionParser parser) {
    parser
        .accepts(FILTER_QUERY, "The WHERE clause of the SPARQL query to " +
            "filter nodes/edges with from the summary graph.")
        .withRequiredArg().ofType(File.class).required();
  }

  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(OptionSet options)
  throws Exception {
    // CASCADE_CONFIG
    final Map<String, Properties> cascadeProperties = cascadeConf.getFlowsConfiguration();
    final Properties properties = cascadeProperties.get(CascadeConfYAMLoader.DEFAULT_PARAMETERS);

    AppProps.setApplicationJarClass(properties, NodeFilterSummaryGraph.class);

    final File fq = (File) options.valueOf(FILTER_QUERY);
    final String filterQuery = getFilterQuery(fq);

    final Tap source = new Hfs(new TextLine(new Fields("quad")), input.get(0));
    final Tap sink = new Hfs(new TextLine(new Fields("quad")), output.get(0));

    final HadoopFlowConnector fc = new HadoopFlowConnector(properties);
    final Flow flow = fc.connect(Analytics.getName(NodeFilterSummaryGraph.class),
      source, sink, new NodeFilterSummaryGraph(filterQuery));
    flow.complete();
    return flow;
  }

  /**
   * Returns the SPARQL query contained in the file as a String.
   */
  private static String getFilterQuery(final File fq)
  throws IOException {
    final StringBuilder sb = new StringBuilder();
    final BufferedReader r = new BufferedReader(new FileReader(fq));
    String line = null;

    try {
      while ((line = r.readLine()) != null) {
        sb.append(line).append('\n');
      }
    } finally {
      r.close();
    }
    return sb.toString();
  }

  public static void main(String[] args)
  throws Exception {
    final NodeFilterSummaryGraphCLI cli = new NodeFilterSummaryGraphCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
