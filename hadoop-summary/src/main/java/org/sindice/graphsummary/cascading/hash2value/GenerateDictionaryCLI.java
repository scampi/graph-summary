/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.hash2value;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;
import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.CascadeConfYAMLoader;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.scheme.HFileScheme;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.management.UnitOfWork;
import cascading.property.AppProps;
import cascading.scheme.hadoop.SequenceFile;
import cascading.stats.CascadingStats;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

/**
 * 
 */
public class GenerateDictionaryCLI extends AbstractAnalyticsCLI {

  private static final Logger logger = LoggerFactory.getLogger(GenerateDictionaryCLI.class);

  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(final OptionSet options)
  throws Exception {
    final Properties properties = cascadeConf.getFlowsConfiguration()
    .get(CascadeConfYAMLoader.DEFAULT_PARAMETERS);
    AppProps.setApplicationJarClass(properties, GenerateDictionaryCLI.class);

    final Tap<JobConf, RecordReader, OutputCollector> source = new Hfs(
      getInputScheme(Analytics.getHeadFields(GenerateDictionary.class)), input.get(0));
    final Tap<JobConf, RecordReader, OutputCollector> sink = new Hfs(
      new SequenceFile(Analytics.getTailFields(GenerateDictionary.class)), output.get(0));

    AppProps.setApplicationJarClass(properties, GenerateDictionary.class);
    final AnalyticsSubAssembly dict = new GenerateDictionary();

    /*
     * https://dev.deri.ie:8443/jira/browse/SND-2682
     * 
     * When this issue is resolved, please update this to a Factory Design
     * Pattern
     */
    final Tap typeDictOut = new Hfs(new HFileScheme(new Fields("hash", "string"), GenerateDictionary.CLASS_PREFIX),
      output.get(0) + "/type-dictionary", SinkMode.REPLACE);
    final Tap predicateDictOut = new Hfs(new HFileScheme(new Fields("hash", "string"), GenerateDictionary.PREDICATE_PREFIX),
      output.get(0) + "/predicate-dictionary", SinkMode.REPLACE);
    final Tap domainDictOut = new Hfs(new HFileScheme(new Fields("hash", "string"), GenerateDictionary.DOMAIN_PREFIX),
      output.get(0) + "/domain-dictionary", SinkMode.REPLACE);
    final Tap datatypeDictOut = new Hfs(new HFileScheme(new Fields("hash", "string"), GenerateDictionary.DATATYPE_PREFIX),
      output.get(0) + "/datatype-dictionary", SinkMode.REPLACE);

    final Map<String, Tap> sinks = new HashMap<String, Tap>();
    sinks.put("type", typeDictOut);
    sinks.put("predicate", predicateDictOut);
    sinks.put("domain", domainDictOut);
    sinks.put("datatype", datatypeDictOut);
    final Flow<JobConf> flow = new HadoopFlowConnector(properties).connect(
      Analytics.getName(GenerateDictionary.class), source, sinks, dict);

    logger.info("Start pruducing mapping between hash and strings");

    flow.complete();
    return flow;
  }

  public static void main(final String[] args) throws Exception {
    final GenerateDictionaryCLI cli = new GenerateDictionaryCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
