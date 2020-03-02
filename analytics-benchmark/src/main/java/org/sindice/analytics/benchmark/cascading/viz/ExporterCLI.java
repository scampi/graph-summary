/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz;

import java.io.File;
import java.util.List;
import java.util.Properties;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.ValueConverter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.analytics.benchmark.cascading.viz.Formatter.FormatterType;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;
import org.sindice.core.analytics.cascading.CascadeConfYAMLoader;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.management.UnitOfWork;

public class ExporterCLI extends AbstractAnalyticsCLI {

  private static final String ROUND = "round";
  private static final String FORMATTER_TYPE = "formatter-type";
  private static final String EXPORTER = "exporter";

  @Override
  protected UnitOfWork<?> doRun(OptionSet options)
  throws Exception {
    final Properties properties = cascadeConf.getFlowsConfiguration().get(CascadeConfYAMLoader.DEFAULT_PARAMETERS);

    BenchmarkResults.setRound((Integer) options.valueOf(ROUND));
    final FormatterType ft = (FormatterType) options.valueOf(FORMATTER_TYPE);
    final FlowProcess fp = new HadoopFlowProcess(HadoopUtil.createJobConf(properties, new JobConf()));
    final ResultsExporter[] exporters = ((List<ResultsExporter>) options.valuesOf(EXPORTER)).toArray(new ResultsExporter[0]);
    final Exporter exp = new Exporter();

    for (String inDir : input) {
      exp.export(ft, new File(inDir), new File(inDir), fp, exporters);
    }
    return null;
  }

  @Override
  protected void initializeOptionParser(OptionParser parser) {
    parser.accepts(FORMATTER_TYPE, "The type of formatter to use")
          .withRequiredArg().ofType(FormatterType.class).defaultsTo(FormatterType.LATEX);
    parser.accepts(EXPORTER, "The exporter to use for the given data")
          .withRequiredArg().withValuesConvertedBy(new ValueConverter<ResultsExporter>() {
            @Override
            public ResultsExporter convert(String value) {
              try {
                Class<? extends ResultsExporter> clazz = (Class<? extends ResultsExporter>) Class.forName(value);
                return clazz.newInstance();
              } catch (Exception e) {
                throw new RuntimeException("Unable to create ResultsExporter instance", e);
              }
            }
            @Override
            public Class<ResultsExporter> valueType() {
              return ResultsExporter.class;
            }
            @Override
            public String valuePattern() {
              return null;
            }
          }).withValuesSeparatedBy(',').required();
    parser.accepts(ROUND, "The rounding decimal precision")
          .withRequiredArg().ofType(Integer.class).defaultsTo(2);
  }

  public static void main(String[] args) throws Exception {
    final ExporterCLI cli = new ExporterCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
