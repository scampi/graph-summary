/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.statistics;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractExportCLI {

  private static final Logger   logger = LoggerFactory
                                       .getLogger(AbstractExportCLI.class);

  private OptionParser          parser;
  private static final String   HELP   = "help";
  protected static final String INPUT  = "input";

  public AbstractExportCLI() {
    parser = this.initializeOptionParser();
    this.initializeOptionParser(parser);
  }

  protected void initializeOptionParser(OptionParser parser) {
    // nothing
  }

  /**
   * Initialise the {@link OptionParser} with the generic Analytics options.
   */
  private OptionParser initializeOptionParser() {
    final OptionParser parser = new OptionParser() {
      {
        accepts(HELP, "shows this help message");
        accepts(INPUT, "Input path. When multi-valued, the order is "
                       + "important for the executing job.").withRequiredArg()
        .describedAs("DIR").ofType(String.class).required();
      }
    };
    return parser;
  }

  public void run(String[] args)
  throws Exception {
    OptionSet options = null;

    try {
      options = parser.parse(args);
    } catch (final OptionException e) {
      logger.error("", e);
      parser.printHelpOn(System.err);
      System.exit(1);
    }

    if (options.has(HELP)) {
      parser.printHelpOn(System.out);
      System.exit(0);
    }

    try {
      doRun((String) options.valueOf(INPUT));
    } catch (Exception e) {
      parser.printHelpOn(System.err);
      throw new RuntimeException(e);
    }
  }

  protected abstract void doRun(String input)
  throws Exception;
}
