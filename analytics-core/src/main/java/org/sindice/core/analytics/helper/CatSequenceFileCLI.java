/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.helper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;
import org.sindice.core.analytics.helper.CatSequenceFile.Display;
import org.sindice.core.analytics.helper.CatSequenceFile.Filter;
import org.sindice.core.analytics.helper.CatSequenceFile.FilterEnum;

import cascading.management.UnitOfWork;
import cascading.stats.CascadingStats;

public class CatSequenceFileCLI extends AbstractAnalyticsCLI {

  private final String INPUT_REGEX = "regex";
  private final String FILTERS = "filters";
  private final String FIELD_OP = "fields-op";
  private final String DISPLAY = "display";
  private final String SEP = "sep";
  private final String MERGE_FIELDS = "merge";

  private String              inputRegex;
  private Map<String, Filter> filters      = new HashMap<String, Filter>();
  private Map<String, Filter> fieldsOp     = new HashMap<String, Filter>();
  private List<String[]>      merges       = null;

  @Override
  protected void initializeOptionParser(OptionParser parser) {
    parser.accepts(INPUT_REGEX, "Regex to filter the input files")
        .withRequiredArg().ofType(String.class).defaultsTo("part-.*");
    parser.accepts(MERGE_FIELDS, "Merge fields values").withRequiredArg().ofType(String.class)
    .withValuesSeparatedBy(',');
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
    parser.accepts(DISPLAY, "How to display the content").withRequiredArg()
        .ofType(Display.class).defaultsTo(Display.PRETTY_PRINT);
    parser.accepts(SEP, "The character separator of the fields")
        .withRequiredArg().ofType(String.class).defaultsTo("");
  }

  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(OptionSet options)
  throws Exception {
    // INPUT_REGEX
    inputRegex = (String) options.valueOf(INPUT_REGEX);
    CatSequenceFile.DISPLAY = (Display) options.valueOf(DISPLAY);
    CatSequenceFile.SEP = (String) options.valueOf(SEP);

    final CatSequenceFile cat = new CatSequenceFile(getConf(), new Path(input.get(0)), inputRegex);

    // FILTERS
    if (options.has(FILTERS)) {
      for (Object f : options.valuesOf(FILTERS)) {
        final String value = (String) f;
        final int must = value.indexOf('+');
        final int mustNot = value.indexOf('-');
        if (must == -1 && mustNot == -1) {
          throw new RuntimeException("malformated filter: " + value);
        }

        final Filter filter = cat.new Filter();
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
    if (options.has(FIELD_OP)) {
      for (Object f : options.valuesOf(FIELD_OP)) {
        final String value = (String) f;
        final int equal = value.indexOf("==");
        final int notEqual = value.indexOf("!=");
        if (equal == -1 && notEqual == -1) {
          throw new RuntimeException("malformated filter: " + value);
        }

        final Filter filter = cat.new Filter();
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

    // MERGE_FIELDS
    if (options.has(MERGE_FIELDS)) {
      final List<String> fields = (List<String>) options.valuesOf(MERGE_FIELDS);
      merges = new ArrayList<String[]>();
      for (String field: fields) {
        merges.add(field.split("\\+"));
      }
    }
    cat.cat((InputFormat) options.valueOf(INPUT_FORMAT), filters, fieldsOp, merges);
    return null;
  }

  public static void main(String[] args)
  throws Exception {
    final CatSequenceFileCLI cli = new CatSequenceFileCLI();
    int res = ToolRunner.run(new JobConf(), cli, args);
    System.exit(res);
  }

}
