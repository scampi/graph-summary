/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.precision.schema;

import java.util.Properties;

import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.analytics.benchmark.cascading.precision.Linksets;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;
import org.sindice.core.analytics.cascading.CascadeConfYAMLoader;
import org.sindice.core.analytics.cascading.annotation.Analytics;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.management.UnitOfWork;
import cascading.property.AppProps;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class SchemaAssemblyCLI extends AbstractAnalyticsCLI {

  public final static String RESULTS = "schema";

  @Override
  protected UnitOfWork<?> doRun(OptionSet options)
  throws Exception {
    final Properties properties = cascadeConf.getFlowsConfiguration().get(CascadeConfYAMLoader.DEFAULT_PARAMETERS);

    AppProps.setApplicationJarClass(properties, SchemaAssembly.class);

    final FlowConnector fc = new HadoopFlowConnector(properties);

    final Fields head = Analytics.getTailsFields(Linksets.class).get("gold-eval-table");
    final Fields tail = Analytics.getTailFields(SchemaAssembly.class);
    final Hfs evalLinkset = new Hfs(getInputScheme(head), input.get(0) + "/gold-eval-table", SinkMode.REPLACE);
    final Hfs goldRelations = new Hfs(new SequenceFile(tail), output.get(0) + "/" + RESULTS);

    final Flow<?> flow = fc.connect(Analytics.getName(SchemaAssembly.class),
      evalLinkset, goldRelations, new SchemaAssembly());
    flow.complete();
    return flow;
  }

  public static void main(String[] args) throws Exception {
    final SchemaAssemblyCLI cli = new SchemaAssemblyCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
