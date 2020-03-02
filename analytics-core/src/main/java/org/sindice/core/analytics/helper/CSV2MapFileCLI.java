/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.helper;

import java.io.File;
import java.io.IOException;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSV2MapFileCLI {

  private final Logger logger = LoggerFactory.getLogger(CSV2MapFileCLI.class);

  private final OptionParser parser;
  private OptionSet opts;

  private final String HELP = "help";
  private final String INPUT = "input";
  private final String OUTPUT = "output";
  private final String SEP_REGEX = "sep-regex";
  private final String KEY_FIELDS = "key-fields";
  private final String VAL_FIELDS = "val-fields";
  private final String KEY_SEP = "key-sep";
  private final String VAL_SEP = "val-sep";
  private final String KEY_TRAILING = "key-trailing-sep";
  private final String VAL_TRAILING = "val-trailing-sep";
  private final String GZIP = "gzip";

  private File input;
  private String output;
  private String sepRegex;
  private Integer[] keyFields;
  private Integer[] valFields;
  private String keySep;
  private String valSep;
  private Boolean keyTrailingSep;
  private Boolean valTrailingSep;

  public CSV2MapFileCLI() {
    parser = new OptionParser();
    parser.accepts(HELP, "Print this help.");
    parser.accepts(INPUT, "The folder with the csv files").withRequiredArg()
        .ofType(File.class);
    parser.accepts(OUTPUT, "The output mapfile name").withRequiredArg()
        .ofType(String.class);
    parser
        .accepts(SEP_REGEX, "The regex string that will be used as separator")
        .withRequiredArg().ofType(String.class);
    parser
        .accepts(KEY_FIELDS,
            "The fields of the csv files that will make the key of the mapfile")
        .withRequiredArg().ofType(Integer.class).withValuesSeparatedBy(',');
    parser
        .accepts(VAL_FIELDS,
            "The fields of the csv files that will make the value of the mapfile")
        .withRequiredArg().ofType(Integer.class).withValuesSeparatedBy(',');
    parser.accepts(GZIP, "the csv files are gzip compressed");
    parser
        .accepts(KEY_SEP,
            "a string that will be used to separate the elements of the keys")
        .withRequiredArg().ofType(String.class).defaultsTo(" ");
    parser
        .accepts(VAL_SEP,
            "a string that will be used to separate the elements of the values")
        .withRequiredArg().ofType(String.class).defaultsTo(" ");
    parser.accepts(KEY_TRAILING,
        "The key separator string is outputed at the end of the key");
    parser.accepts(VAL_TRAILING,
        "The value separator string is outputed at the end of the value");
  }

  private void printError(final String opt) throws IOException {
    parser.printHelpOn(System.out);
    throw new IOException("Missing option: " + opt);
  }

  public final void parseAndExecute(final String[] cmds) throws IOException {
    opts = parser.parse(cmds);
    if (opts.has(HELP)) {
      parser.printHelpOn(System.out);
      return;
    }

    // GZIP
    CSV2MapFile.USE_GZIP = opts.has(GZIP);
    // KEY_SEP
    keySep = (String) opts.valueOf(KEY_SEP);
    // VAL_SEP
    valSep = (String) opts.valueOf(VAL_SEP);
    // KEY_SEP
    keyTrailingSep = opts.has(KEY_TRAILING);
    // VAL_SEP
    valTrailingSep = opts.has(VAL_TRAILING);

    // INPUT
    if (opts.has(INPUT)) {
      input = (File) opts.valueOf(INPUT);
    } else {
      printError(INPUT);
    }
    // OUTPUT
    if (opts.has(OUTPUT)) {
      output = (String) opts.valueOf(OUTPUT);
    } else {
      printError(OUTPUT);
    }
    // SEP_REGEX
    if (opts.has(SEP_REGEX)) {
      sepRegex = (String) opts.valueOf(SEP_REGEX);
    } else {
      printError(SEP_REGEX);
    }
    // KEY_FIELDS
    if (opts.has(KEY_FIELDS)) {
      keyFields = ((List<Integer>) opts.valuesOf(KEY_FIELDS))
          .toArray(new Integer[0]);
    } else {
      printError(KEY_FIELDS);
    }
    // VAL_FIELDS
    if (opts.has(VAL_FIELDS)) {
      valFields = ((List<Integer>) opts.valuesOf(VAL_FIELDS))
          .toArray(new Integer[0]);
    } else {
      printError(VAL_FIELDS);
    }

    logger.info("Creating MapFile in {} from csv files at {}", input, output);
    final CSV2MapFile csv = new CSV2MapFile(input, output);
    csv.setKeyFields(keyFields);
    csv.setValueFields(valFields);
    csv.toMapFile(sepRegex, keySep, valSep, keyTrailingSep, valTrailingSep);
  }

  public static void main(String[] args) throws IOException {
    final CSV2MapFileCLI cli = new CSV2MapFileCLI();

    cli.parseAndExecute(args);
  }

}
