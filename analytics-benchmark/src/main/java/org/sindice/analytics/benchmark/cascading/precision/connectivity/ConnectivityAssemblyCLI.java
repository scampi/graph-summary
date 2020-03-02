/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.precision.connectivity;

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

public class ConnectivityAssemblyCLI extends AbstractAnalyticsCLI {

  /** The name of the folder with the results */
  public static final String RESULTS = "tf-positive";

  @Override
  protected UnitOfWork<?> doRun(OptionSet options)
  throws Exception {
    final Properties properties = cascadeConf.getFlowsConfiguration().get(CascadeConfYAMLoader.DEFAULT_PARAMETERS);

    // Compute true false positive

    AppProps.setApplicationJarClass(properties, ConnectivityAssembly.class);

    final FlowConnector fc = new HadoopFlowConnector(properties);

    final Fields head = Analytics.getTailsFields(Linksets.class).get("eval-linkset");
    final Fields tail = Analytics.getTailFields(ConnectivityAssembly.class);
    final Hfs evalLinkset = new Hfs(getInputScheme(head), input.get(0) + "/eval-linkset");
    final Hfs goldRelations = new Hfs(new SequenceFile(tail), output.get(0) + "/" + RESULTS, SinkMode.REPLACE);

    final Flow<?> flow = fc.connect(Analytics.getName(ConnectivityAssembly.class),
      evalLinkset, goldRelations, new ConnectivityAssembly());
    flow.complete();
    return flow;
  }

  public static void main(String[] args) throws Exception {
    final ConnectivityAssemblyCLI cli = new ConnectivityAssemblyCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
