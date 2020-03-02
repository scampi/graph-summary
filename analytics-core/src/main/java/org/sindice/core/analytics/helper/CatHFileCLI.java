/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.helper;

import java.io.IOException;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CatHFileCLI
extends Configured
implements Tool {

  private final OptionParser parser;
  private OptionSet          opts;

  private final String       HELP      = "help";
  private final String       INPUT     = "input";
  private final String       KEY_CLASS = "key-class";
  private final String       AS_BYTES  = "as-bytes";

  private String             input;

  public CatHFileCLI() {
    parser = new OptionParser();
    parser.accepts(HELP, "Print this help.");
    parser.accepts(KEY_CLASS, "The class of the keys").withRequiredArg().ofType(String.class);
    parser.accepts(INPUT, "The hfile").withRequiredArg().ofType(String.class);
    parser.accepts(AS_BYTES, "Show the key-value as byte[] arrays");
  }

  private void printError(final String opt)
  throws IOException {
    parser.printHelpOn(System.out);
    throw new IOException("Missing option: " + opt);
  }

  @Override
  public int run(String[] args)
  throws Exception {
    opts = parser.parse(args);
    if (opts.has(HELP)) {
      parser.printHelpOn(System.out);
      return 1;
    }

    // INPUT
    if (opts.has(INPUT)) {
      input = (String) opts.valueOf(INPUT);
    } else
      printError(INPUT);

    final CatHFile hfile = new CatHFile();
    hfile.cat(getConf(), new Path(input), (String) opts.valueOf(KEY_CLASS), opts.has(AS_BYTES));
    return 0;
  }

  public static void main(String[] args)
  throws Exception {
    final CatHFileCLI cli = new CatHFileCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
