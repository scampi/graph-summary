/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.helper;

import java.io.File;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;

import cascading.management.UnitOfWork;
import cascading.stats.CascadingStats;

public class SplitToSequenceFileCLI
extends AbstractAnalyticsCLI {

  private static final String MAX_FILE_SIZE = "max-size";
  private static final String EXTENSIONS = "extensions";

  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(OptionSet options)
  throws Exception {
    final String[] extensions = options.has(EXTENSIONS) ?
                               ((List<String>) options.valuesOf(EXTENSIONS)).toArray(new String[0])
                               : null;
    final long max = (Long) options.valueOf(MAX_FILE_SIZE);
    final SplitToSequenceFile split = new SplitToSequenceFile((JobConf) getConf(), max);
    split.split(new File(input.get(0)), extensions, new File(output.get(0)));
    return null;
  }

  @Override
  protected void initializeOptionParser(OptionParser parser) {
    parser.accepts(MAX_FILE_SIZE, "The maximum size in bytes of a split")
          .withRequiredArg().ofType(Long.class).defaultsTo(64L * 1024 * 1024);
    parser.accepts(EXTENSIONS, "The list of accepted file extensions. If not specified, accepts all.")
          .withRequiredArg().ofType(String.class).withValuesSeparatedBy(',');
  }

  public static void main(String[] args)
  throws Exception {
    final SplitToSequenceFileCLI cli = new SplitToSequenceFileCLI();
    int res = ToolRunner.run(new JobConf(), cli, args);
    System.exit(res);
  }

}
