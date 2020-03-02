/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.precision;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
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
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class LinksetsCLI extends AbstractAnalyticsCLI {

  @Override
  protected UnitOfWork<?> doRun(OptionSet options)
  throws Exception {
    final Properties properties = cascadeConf.getFlowsConfiguration().get(CascadeConfYAMLoader.DEFAULT_PARAMETERS);

    AppProps.setApplicationJarClass(properties, Linksets.class);

    final FlowConnector fc = new HadoopFlowConnector(properties);

    final Map<String, Fields> heads = Analytics.getHeadsFields(Linksets.class);
    final Hfs goldRelations = new Hfs(getInputScheme(heads.get("gold-relations")), input.get(0) + "/relationsGraph");
    final Hfs goldProps = new Hfs(getInputScheme(heads.get("gold-properties")), input.get(0) + "/getPropertiesGraph");
    final Hfs goldTable = new Hfs(getInputScheme(heads.get("gold-entity-table")), input.get(0) + "/clusterGeneratorGraph");
    final Hfs evalRelations = new Hfs(getInputScheme(heads.get("eval-relations")), input.get(1) + "/relationsGraph");
    final Hfs evalProps = new Hfs(getInputScheme(heads.get("eval-properties")), input.get(1) + "/getPropertiesGraph");
    final Hfs evalTable = new Hfs(getInputScheme(heads.get("eval-entity-table")), input.get(1) + "/clusterGeneratorGraph");

    final Map<String, Fields> tails = Analytics.getTailsFields(Linksets.class);
    final Hfs evalLinkset = new Hfs(new SequenceFile(tails.get("eval-linkset")), output.get(0) + "/eval-linkset", SinkMode.REPLACE);
    final Hfs goldEvalTable = new Hfs(new SequenceFile(tails.get("gold-eval-table")), output.get(0) + "/gold-eval-table", SinkMode.REPLACE);

    final Map<String, Tap> sources = new HashMap<String, Tap>();
    sources.put("gold-relations", goldRelations);
    sources.put("gold-properties", goldProps);
    sources.put("gold-entity-table", goldTable);
    sources.put("eval-relations", evalRelations);
    sources.put("eval-properties", evalProps);
    sources.put("eval-entity-table", evalTable);
    final Map<String, Tap> sinks = new HashMap<String, Tap>();
    sinks.put("eval-linkset", evalLinkset);
    sinks.put("gold-eval-table", goldEvalTable);

    final Flow<?> flow = fc.connect(Analytics.getName(Linksets.class), sources, sinks, new Linksets());
    flow.complete();
    return flow;
  }

  public static void main(String[] args) throws Exception {
    final LinksetsCLI cli = new LinksetsCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
