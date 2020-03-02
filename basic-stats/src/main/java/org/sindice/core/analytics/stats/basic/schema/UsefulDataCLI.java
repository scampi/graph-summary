/*******************************************************************************
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 *
 *
 * This project is a free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This project is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this project. If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.sindice.core.analytics.stats.basic.schema;

import java.io.IOException;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Diego Ceccarelli <diego.ceccarelli@deri.org>
 * @link www.di.unipi.it/~ceccarel
 */
public class UsefulDataCLI {

  private static final Logger logger = LoggerFactory
                                     .getLogger(UsefulDataCLI.class);

  private static final String HELP   = "help";
  private static final String INPUT  = "input";
  private static final String OUTPUT = "output";

  private final OptionParser  parser;
  private String              input;
  private String              output;

  public UsefulDataCLI() {
    parser = new OptionParser() {
      {
        this.accepts(HELP, "shows this help message");
      }
    };
    parser.accepts(INPUT, "original file having the statistics")
    .withRequiredArg().ofType(String.class);
    parser.accepts(OUTPUT, "file containing class/predicate names having schema.org in them will be output")
    .withRequiredArg().ofType(String.class);
  }

  private void printMissingOptionError(String opt) {
    System.err.println("Error: Missing option " + opt);
    System.err.println("");
    try {
      parser.printHelpOn(System.err);
    } catch (IOException e) {
      e.printStackTrace();
    }
    throw new RuntimeException();
  }

  private void parseAndExecute(final String[] args)
  throws IOException {
    final OptionSet options;

    try {
      options = parser.parse(args);
    } catch (final OptionException e) {
      System.err.println("Error: " + e.getMessage());
      System.err.println("");
      parser.printHelpOn(System.err);
      System.exit(1);
      return;
    }

    if (options.has(HELP)) {
      parser.printHelpOn(System.out);
      System.exit(0);
    }

    // INPUT
    if (options.has(INPUT)) {
      input = (String) options.valueOf(INPUT);
    } else {
      printMissingOptionError(INPUT);
    }
    // OUTPUT
    if (options.has(OUTPUT)) {
      output = (String) options.valueOf(OUTPUT);
    } else {
      printMissingOptionError(OUTPUT);
    }

    logger.info("Starting the computation of Schema.org stats from {}", input);
    final UsefulData use = new UsefulData();
    final int count = use.compute(input, output);
    logger.debug("Count = {}", count);
  }

  public static void main(final String[] args)
  throws Exception {
    final UsefulDataCLI cli = new UsefulDataCLI();
    cli.parseAndExecute(args);
  }

}
