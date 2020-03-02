/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.hash2value.rdf;

import java.util.Properties;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;
import org.sindice.core.analytics.cascading.CascadeConfYAMLoader;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.scheme.ExtensionTextLine;
import org.sindice.graphsummary.cascading.DataGraphSummaryCascade;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.management.UnitOfWork;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextLine.Compress;
import cascading.stats.CascadingStats;
import cascading.tap.hadoop.Hfs;

public class RDFRelationsGraphCLI
extends AbstractAnalyticsCLI {

  private static final String PREDICATE_DICT = "predicate-dict";
  private static final String DOMAIN_DICT    = "domain-dict";
  private static final String TYPE_DICT      = "type-dict";
  private static final String DATATYPE_DICT  = "datatype-dict";

  @Override
  protected void initializeOptionParser(final OptionParser parser) {
    parser.accepts(PREDICATE_DICT, "The predicate dictionary")
          .withRequiredArg().describedAs("ARCHIVE").ofType(String.class).required();
    parser.accepts(DOMAIN_DICT, "The domain dictionary")
          .withRequiredArg().describedAs("ARCHIVE").ofType(String.class).required();
    parser.accepts(TYPE_DICT, "The type dictionary")
          .withRequiredArg().describedAs("ARCHIVE").ofType(String.class).required();
    parser.accepts(DATATYPE_DICT, "The datatype dictionary")
          .withRequiredArg().describedAs("ARCHIVE").ofType(String.class).required();
  }

  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(final OptionSet options)
  throws Exception {
    final Properties properties = cascadeConf.getFlowsConfiguration().get(CascadeConfYAMLoader.DEFAULT_PARAMETERS);

    final String predicateDict = (String) options.valueOf(PREDICATE_DICT);
    final String domainDict = (String) options.valueOf(DOMAIN_DICT);
    final String typeDict = (String) options.valueOf(TYPE_DICT);
    final String datatypeDict = (String) options.valueOf(DATATYPE_DICT);

    // Distribute the dictionaries
    DataGraphSummaryCascade.addDictionariesToCache(properties, typeDict, predicateDict, domainDict, datatypeDict);

    AppProps.setApplicationJarClass(properties, RDFRelationsGraph.class);
    final RDFRelationsGraph rel = new RDFRelationsGraph();
    final FlowConnector rdfRG = new HadoopFlowConnector(properties);
    final Hfs source = new Hfs(getInputScheme(Analytics.getHeadFields(RDFRelationsGraph.class)), input.get(0));
    final ExtensionTextLine scheme = new ExtensionTextLine("nq", Compress.ENABLE);
    final Hfs sink = new Hfs(scheme, output.get(0));
    final Flow<?> flow = rdfRG.connect(Analytics.getName(RDFRelationsGraph.class),
      source, sink, rel);
    flow.complete();
    return flow;
  }

  public static void main(String[] args)
  throws Exception {
    final RDFRelationsGraphCLI cli = new RDFRelationsGraphCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
