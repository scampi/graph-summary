/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.helper;

import java.util.Properties;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;
import org.sindice.core.analytics.cascading.CascadeConfYAMLoader;
import org.sindice.core.analytics.cascading.operation.InsertRandomValue;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.management.UnitOfWork;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.SequenceFile;
import cascading.stats.CascadingStats;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

/**
 * This utility class compresses the data input.
 * The number of output files can be controlled via the Hadoop parameter <b>mapred.reduce.tasks</b>.
 */
public class CompressSequenceFile
extends AbstractAnalyticsCLI {

  private final static String NB_FIELDS = "nb-fields";

  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(OptionSet options)
  throws Exception {
    final String[] fields = new String[(Integer) options.valueOf(NB_FIELDS)];

    for (int i = 0; i < fields.length; i++) {
      fields[i] = "field" + i;
    }

    final Properties properties = cascadeConf.getFlowsConfiguration().get(CascadeConfYAMLoader.DEFAULT_PARAMETERS);
    AppProps.setApplicationJarClass(properties, CompressSequenceFile.class);
    final Tap source = new Hfs(getInputScheme(new Fields(fields)), input.get(0));
    final Tap sink = new Hfs(new SequenceFile(new Fields(fields)), output.get(0));

    Pipe pipe = new Each("compress", new Fields(fields), new Identity());
    pipe = new Each(pipe, new InsertRandomValue(new Fields("d")), Fields.ALL);
    pipe = new GroupBy(pipe, new Fields("d"));

    final FlowConnector fc = new HadoopFlowConnector(properties);
    final Flow flow = fc.connect("compress", source, sink, pipe);
    flow.complete();
    return flow;
  }

  @Override
  protected void initializeOptionParser(OptionParser parser) {
    parser.accepts(NB_FIELDS, "The number of fields in the input data")
          .withRequiredArg().ofType(Integer.class).defaultsTo(1);
  }

  public static void main(String[] args)
  throws Exception {
    final CompressSequenceFile cli = new CompressSequenceFile();
    int res = ToolRunner.run(new JobConf(), cli, args);
    System.exit(res);
  }

}
