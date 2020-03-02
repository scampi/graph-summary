/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.stats.basic;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;

import cascading.cascade.Cascade;
import cascading.management.UnitOfWork;
import cascading.scheme.Scheme;
import cascading.stats.CascadingStats;
import cascading.tuple.Fields;

public class BasicStatsCascadeCLI extends AbstractAnalyticsCLI {

  public enum BasicStats {
    CLASS, PREDICATE, URI, FORMAT, NAMESPACE
  }

  private static final String STAT = "stat";
  private static final String DATE = "date";

  @Override
  protected void initializeOptionParser(final OptionParser parser) {
    parser.accepts(STAT, "The statistics to compute " + Arrays.toString(BasicStats.values()))
    .withRequiredArg().ofType(BasicStats.class).required();
    parser.accepts(DATE, "The date of the sindice export the statistics are computed on")
    .withRequiredArg().ofType(String.class).required();
  }

  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(final OptionSet options)
      throws Exception {
    // CASCADE_CONFIG
    final Map<String, Properties> props = cascadeConf.getFlowsConfiguration(BasicStatsCascade.FLOWS_NAME);
    // DATE
    final String date = (String) options.valueOf(DATE);
    props.get(BasicStatsCascade.RDF_FLOW).setProperty("date", date);

    final Scheme iScheme = getInputScheme(new Fields("value"));
    final Cascade cascade;
    switch ((BasicStats) options.valueOf(STAT)) {
    case CLASS:
      cascade = BasicStatsCascade.getClassCascade(input.get(0), iScheme, output.get(0), props);
      break;
    case FORMAT:
      cascade = BasicStatsCascade.getFormatCascade(input.get(0), iScheme, output.get(0), props);
      break;
    case NAMESPACE:
      cascade = BasicStatsCascade.getNamespaceCascade(input.get(0), iScheme, output.get(0), props);
      break;
    case PREDICATE:
      cascade = BasicStatsCascade.getPredicateCascade(input.get(0), iScheme, output.get(0), props);
      break;
    case URI:
      cascade = BasicStatsCascade.getUriCascade(input.get(0), iScheme, output.get(0), props);
      break;
    default:
      throw new EnumConstantNotPresentException(BasicStats.class, options.valueOf(STAT).toString());
    }
    cascade.complete();
    return cascade;
  }

  public static void main(String[] args) throws Exception {
    final BasicStatsCascadeCLI cli = new BasicStatsCascadeCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
