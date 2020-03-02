/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.stats.basic.cli;

import java.util.Properties;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;
import org.sindice.core.analytics.cascading.CascadeConfYAMLoader;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.scheme.ExtensionTextLine;
import org.sindice.core.analytics.stats.basic.rdf.RDFCollectionFormatStats;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.management.UnitOfWork;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextLine.Compress;
import cascading.stats.CascadingStats;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

public class RDFCollectionFormatStatsCLI extends AbstractAnalyticsCLI {

  private static final String DATE = "date";

  @Override
  protected void initializeOptionParser(final OptionParser parser) {
    parser.accepts(DATE, "Date in the form YYYY-MM-DD").withRequiredArg()
        .describedAs("Date").ofType(String.class);
  }

  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(final OptionSet options)
  throws Exception {
    final Properties properties = cascadeConf.getFlowsConfiguration()
                                             .get(CascadeConfYAMLoader.DEFAULT_PARAMETERS);

    if (options.has(DATE)) {
      properties.setProperty("date", (String) options.valueOf(DATE));
    } else {
      printMissingOptionError(DATE);
    }

    AppProps.setApplicationJarClass(properties, RDFCollectionFormatStatsCLI.class);

    final Tap source = new Hfs(getInputScheme(Analytics.getHeadFields(RDFCollectionFormatStats.class)), input.get(0));
    final Tap sink = new Hfs(new ExtensionTextLine("nq", Compress.ENABLE), output.get(0),
      SinkMode.REPLACE);
    final RDFCollectionFormatStats pipe = new RDFCollectionFormatStats();
    final Flow<?> flow = new HadoopFlowConnector(properties).connect(
      Analytics.getName(pipe), source, sink, pipe);

    flow.complete();
    return flow;
  }

  public static void main(final String[] args) throws Exception {
    final RDFCollectionFormatStatsCLI cli = new RDFCollectionFormatStatsCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
