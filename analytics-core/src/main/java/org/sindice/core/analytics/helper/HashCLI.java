/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.helper;

import java.io.IOException;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.sindice.core.analytics.util.Hash;

/**
 * This CLI allows to {@link Hash hash} text using a given amount of bits.
 */
public class HashCLI {

  private final OptionParser parser;

  private final String       HELP  = "help";
  private final String       INPUT = "input";
  private final String       SIZE  = "size";

  public HashCLI() {
    parser = new OptionParser();
    parser.accepts(HELP, "Print this help.");
    parser.accepts(SIZE, "The size in bits of the hash: [32, 64, 128]").withRequiredArg().ofType(Integer.class).required();
    parser.accepts(INPUT, "The text to hash").withRequiredArg().ofType(String.class).required();
  }

  public final void parseAndExecute(final String[] cmds)
  throws IOException {
    final OptionSet opts = parser.parse(cmds);

    if (opts.has(HELP)) {
      parser.printHelpOn(System.out);
      return;
    }

    // INPUT
    final String input = (String) opts.valueOf(INPUT);
    final int size = (Integer) opts.valueOf(SIZE);

    if (size == 32) {
      System.out.println(Hash.getHash32(input));
    } else if (size == 64) {
      System.out.println(Hash.getHash64(input));
    } else if (size == 128) {
      System.out.println(Hash.getHash128(input));
    } else {
      throw new IllegalArgumentException("Not supported hash size: [" + size + "]");
    }
  }

  public static void main(String[] args)
  throws IOException {
    final HashCLI cli = new HashCLI();
    cli.parseAndExecute(args);
  }

}
