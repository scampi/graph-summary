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
public class ExtraSchemaCLI {

  private static final Logger logger      = LoggerFactory
                                          .getLogger(ExtraSchemaCLI.class);

  private static final String HELP        = "help";
  private static final String TABLE       = "table";
  private static final String SCHEMA      = "schema";
  private static final String EXTRA_TERMS = "extra-terms";

  private final OptionParser  parser;
  private String              table;
  private String              schema;
  private String              extraTerms;

  public ExtraSchemaCLI() {
    parser = new OptionParser() {
      {
        this.accepts(HELP, "shows this help message");
      }
    };
    parser.accepts(TABLE, "file to be given (included int the folder) as input \"sorted_table.txt\"")
    .withRequiredArg().ofType(String.class);
    parser.accepts(SCHEMA, "Output from the previous run of UsefulData")
    .withRequiredArg().ofType(String.class);
    parser.accepts(EXTRA_TERMS, "this will be given as output by the program, containing the extra terms.")
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

    // TABLE
    if (options.has(TABLE)) {
      table = (String) options.valueOf(TABLE);
    } else {
      printMissingOptionError(TABLE);
    }
    // SCHEMA
    if (options.has(SCHEMA)) {
      schema = (String) options.valueOf(SCHEMA);
    } else {
      printMissingOptionError(SCHEMA);
    }
    // EXTRA_TERMS
    if (options.has(EXTRA_TERMS)) {
      extraTerms = (String) options.valueOf(EXTRA_TERMS);
    } else {
      printMissingOptionError(EXTRA_TERMS);
    }

    logger.info("Starting the computation of extra terms from Schema.org from {} into {}", schema, extraTerms);
    final ExtraSchema extraSchema = new ExtraSchema(table);
    final int extraNb = extraSchema.compute(schema, extraTerms);
    logger.debug("Extra schema.org terms: {}", extraNb);
  }

  public static void main(final String[] args)
  throws Exception {
    final ExtraSchemaCLI cli = new ExtraSchemaCLI();
    cli.parseAndExecute(args);
  }

}
