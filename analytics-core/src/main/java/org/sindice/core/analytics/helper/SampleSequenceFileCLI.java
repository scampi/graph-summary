/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.helper;

import java.io.IOException;
import java.util.HashMap;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.hadoop.fs.Path;
import org.sindice.core.analytics.helper.ExtractSampleSequenceFile.Filter;
import org.sindice.core.analytics.helper.ExtractSampleSequenceFile.FilterEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleSequenceFileCLI {

  private final Logger logger = LoggerFactory
      .getLogger(SampleSequenceFileCLI.class);

  private final OptionParser parser;
  private OptionSet opts;

  private final String HELP = "help";
  private final String INPUT = "input";
  private final String OUTPUT = "output";
  private final String IS_HADOOP = "is-hadoop";
  private final String MAX_TUPLES = "max-tuples";
  private final String SAMPLE = "sample";
  private final String KEY_CLASS = "key-class";
  private final String VAL_CLASS = "val-class";
  private final String REGEX = "regex";

  // Random sample
  private final String PROBABILITY = "probability";
  // Extract sample
  private final String FILTERS = "filters";
  private final String FIELD_OP = "fields-op";
  // Topk
  private final String TOP_K = "topk";

  private Path input;
  private String output;
  private HashMap<String, Filter> filters = new HashMap<String, Filter>();
  private HashMap<String, Filter> fieldsOp = new HashMap<String, Filter>();

  public enum Sample {
    RANDOM, EXTRACT, TOPK
  }

  public SampleSequenceFileCLI() {
    parser = new OptionParser();
    parser.accepts(HELP, "Print this help.");
    parser.accepts(INPUT, "The folder with the sequence files")
        .withRequiredArg().ofType(Path.class);
    parser.accepts(OUTPUT, "The output directory with the sample seqfile")
        .withRequiredArg().ofType(String.class);
    parser
        .accepts(PROBABILITY,
            "Sample the sequence file with a proba of <probabililty>")
        .withRequiredArg().ofType(Float.class).defaultsTo(0.1f);
    parser
        .accepts(IS_HADOOP,
            "the input sequence file is a original hadoop file, not from cascading");
    parser.accepts(MAX_TUPLES, "max number of tuples to get")
        .withRequiredArg().ofType(Integer.class);
    parser.accepts(TOP_K, "sample the topk").withRequiredArg()
        .ofType(Integer.class).defaultsTo(10);
    parser
        .accepts(
            FILTERS,
            "List of field filters, with the format <field>[+|-]<value>(,<field>[+|-]<value>)*, where + is a MUST and - is a NOT.")
        .withRequiredArg().ofType(String.class).withValuesSeparatedBy(',');
    parser
        .accepts(
            FIELD_OP,
            "List of field operators, with the format <field>[=|!]<field>(,<field>[=|!]<field>)*, where = is an equla and ! is a not equal.")
        .withRequiredArg().ofType(String.class).withValuesSeparatedBy(',');
    parser.accepts(SAMPLE, "The sample method to use").withRequiredArg()
        .ofType(Sample.class);
    parser
        .accepts(KEY_CLASS,
            "Key class, in case of a hadoop native sequencefile")
        .withRequiredArg().ofType(String.class)
        .defaultsTo("org.apache.hadoop.io.Text");
    parser
        .accepts(VAL_CLASS,
            "Value class, in case of a hadoop native sequencefile")
        .withRequiredArg().ofType(String.class)
        .defaultsTo("org.apache.hadoop.io.Text");
    parser.accepts(REGEX, "The regex to use for listings the input files")
        .withRequiredArg().ofType(String.class).defaultsTo("^part-.*");
  }

  private void printError(final String opt) throws IOException {
    parser.printHelpOn(System.out);
    throw new IOException("Missing option: " + opt);
  }

  public final void parseAndExecute(final String[] cmds) throws IOException,
      ClassNotFoundException {
    opts = parser.parse(cmds);
    if (opts.has(HELP)) {
      parser.printHelpOn(System.out);
      return;
    }

    // INPUT
    if (opts.has(INPUT)) {
      input = (Path) opts.valueOf(INPUT);
    } else {
      printError(INPUT);
    }
    // OUTPUT
    if (opts.has(OUTPUT)) {
      output = (String) opts.valueOf(OUTPUT);
    } else {
      printError(OUTPUT);
    }
    // FILTERS
    if (opts.has(FILTERS)) {
      for (Object f : opts.valuesOf(FILTERS)) {
        final String value = (String) f;
        final int must = value.indexOf('+');
        final int mustNot = value.indexOf('-');
        if (must == -1 && mustNot == -1) {
          throw new RuntimeException("malformated filter: " + value);
        }

        final Filter filter = new Filter();
        if (must != -1 && mustNot == -1) {
          filter.value = value.substring(must + 1);
          filter.occur = FilterEnum.MUST;
          filters.put(value.substring(0, must), filter);
        } else if (must == -1 && mustNot != -1) {
          filter.value = value.substring(mustNot + 1);
          filter.occur = FilterEnum.MUST_NOT;
          filters.put(value.substring(0, mustNot), filter);
        } else {
          final int smaller = must < mustNot ? must : mustNot;
          filter.value = value.substring(smaller + 1);
          filter.occur = must < mustNot ? FilterEnum.MUST
              : FilterEnum.MUST_NOT;
          filters.put(value.substring(0, smaller), filter);
        }
      }
    }
    // FIELD_OP
    if (opts.has(FIELD_OP)) {
      for (Object f : opts.valuesOf(FIELD_OP)) {
        final String value = (String) f;
        final int equal = value.indexOf("==");
        final int notEqual = value.indexOf("!=");
        if (equal == -1 && notEqual == -1) {
          throw new RuntimeException("malformated filter: " + value);
        }

        final Filter filter = new Filter();
        if (equal != -1 && notEqual == -1) {
          filter.value = value.substring(equal + 2);
          filter.occur = FilterEnum.MUST;
          fieldsOp.put(value.substring(0, equal), filter);
        } else if (equal == -1 && notEqual != -1) {
          filter.value = value.substring(notEqual + 2);
          filter.occur = FilterEnum.MUST_NOT;
          fieldsOp.put(value.substring(0, notEqual), filter);
        } else {
          final int smaller = equal < notEqual ? equal : notEqual;
          filter.value = value.substring(smaller + 2);
          filter.occur = equal < notEqual ? FilterEnum.MUST
              : FilterEnum.MUST_NOT;
          fieldsOp.put(value.substring(0, smaller), filter);
        }
      }
    }

    final AbstractSampleSequenceFile s;
    switch ((Sample) opts.valueOf(SAMPLE)) {
    case RANDOM:
      s = new RandomSampleSequenceFile(input, output,
          (String) opts.valueOf(REGEX), (Float) opts.valueOf(PROBABILITY));
      logger.info(
          "Sample the sequence files at {} into {}, with probability {}",
          new Object[] { input, output, opts.valueOf(PROBABILITY) });
      break;
    case EXTRACT:
      s = new ExtractSampleSequenceFile(input, output,
          (String) opts.valueOf(REGEX), filters, fieldsOp);
      logger.info("Sample the sequence files at {} into {}", new Object[] {
          input, output });
      break;
    case TOPK:
      s = new TopKSampleSequenceFile(input, output,
          (String) opts.valueOf(REGEX), (Integer) opts.valueOf(TOP_K));
      logger.info("Sample the sequence files at {} into {}, with topk {}",
          new Object[] { input, output, opts.valueOf(TOP_K) });
      break;
    default:
      throw new EnumConstantNotPresentException(Sample.class, opts.valueOf(
          SAMPLE).toString());
    }
    if (opts.has(IS_HADOOP)) {
      s.setKeyClass((String) opts.valueOf(KEY_CLASS));
      s.setValClass((String) opts.valueOf(VAL_CLASS));
    }
    s.sample(opts.has(IS_HADOOP),
        opts.has(MAX_TUPLES) ? (Integer) opts.valueOf(MAX_TUPLES) : -1);
  }

  public static void main(String[] args) throws IOException,
      ClassNotFoundException {
    final SampleSequenceFileCLI cli = new SampleSequenceFileCLI();

    cli.parseAndExecute(args);
  }

}
