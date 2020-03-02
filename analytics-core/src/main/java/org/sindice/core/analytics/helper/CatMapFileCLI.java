/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.helper;

import java.io.IOException;

import joptsimple.OptionParser;
import joptsimple.OptionSet;


public class CatMapFileCLI {

  private final OptionParser parser;
  private OptionSet opts;

  private final String HELP = "help";
  private final String INPUT = "input";

  private String input;

  public CatMapFileCLI() {
    parser = new OptionParser();
    parser.accepts(HELP, "Print this help.");
    parser.accepts(INPUT, "The folder with the mapfile").withRequiredArg().ofType(String.class);
  }

  private void printError(final String opt) throws IOException {
    parser.printHelpOn(System.out);
    throw new IOException("Missing option: " + opt);
  }

  public final void parseAndExecute(final String[] cmds)
  throws Exception {
    opts = parser.parse(cmds);
    if (opts.has(HELP)) {
      parser.printHelpOn(System.out);
      return;
    }

    // INPUT
    if (opts.has(INPUT)) {
      input = (String) opts.valueOf(INPUT);
    } else
      printError(INPUT);

    final CatMapFile cm = new CatMapFile(input);
    cm.cat();
  }

  public static void main(String[] args)
  throws Exception {
    final CatMapFileCLI cli = new CatMapFileCLI();

    cli.parseAndExecute(args);
  }

}
