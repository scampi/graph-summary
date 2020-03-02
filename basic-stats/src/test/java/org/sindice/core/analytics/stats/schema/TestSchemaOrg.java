/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.stats.schema;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.stats.basic.assembly.BasicClassStats;
import org.sindice.core.analytics.stats.basic.assembly.BasicPredicateStats;
import org.sindice.core.analytics.stats.basic.schema.ExtraSchema;
import org.sindice.core.analytics.stats.basic.schema.UsefulData;
import org.sindice.core.analytics.testHelper.AbstractAnalyticsTestCase;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

public class TestSchemaOrg extends AbstractAnalyticsTestCase {

  private String createSequenceFile(Properties properties, Object[] expected, Fields fields, File output)
  throws IOException {
    if ((expected.length / fields.size()) * fields.size() != expected.length) {
      throw new IllegalArgumentException("the expected number of tuples did not match the number of fields: got " +
                                         expected.length + " for " + fields.size() + " fields");
    }

    final String fn = new File(output, "sfOut" + Math.random()).getAbsolutePath();
    HadoopFlowProcess flowProcess = new HadoopFlowProcess(HadoopUtil.createJobConf(properties, new JobConf()));
    final TupleEntryCollector w = new Hfs(new SequenceFile(fields), fn).openForWrite(flowProcess);
    try {
      for (int i = 0; i < expected.length; i += fields.size()) {
        w.add(new Tuple(Arrays.copyOfRange(expected, i, i + fields.size())));
      }
    }
    finally {
      w.close();
    }
    return fn;
  }

  @Test
  public void testUsefulData()
  throws Exception {
    final UsefulData ud = new UsefulData();

    final String[] classes = { "\"/computer/x68000\"", "1", "1", "1", "1",
                               "\"/console/nes/home/\"", "1", "1", "1", "1",
                               "\"/portable/ds\"", "9", "9", "1", "1",
                               "\"00ab villa royale 00bb\"", "3", "3", "1", "1",
                               "\"00c0lbum d'estudi\"", "23", "23", "1", "1",
                               "\"00c0lbum de mescles\"", "1", "1", "1", "1",
                               "\"00c9cole d'ing00e9nieurs fran00e7aise\"", "1", "1", "1", "1",
                               "\"00c9cole secondaire priv00e9e\"", "1", "1", "1", "1",
                               "\"00c9cole\"", "6", "6", "1", "1",
                               "\"00c9coles d2019ing00e9nieurs fran00e7aises\"", "1", "1", "1", "1" };
    final String[] predicates = { "aiis:code", "30", "2", "1", "1",
                                  "bui:limitKForWalls", "1", "1", "1", "1",
                                  "dbpprop:website", "10", "2", "1", "1",
                                  "dbr:Atrribute", "2", "1", "1", "1",
                                  "dcterm:title", "4", "4", "2", "2",
                                  "doap:homepage", "1", "1", "1", "1",
                                  "file:///devel/dig/2007/01/exhibit/#valueType", "3", "1", "1", "1",
                                  "file:/home/connolly/w3ccvs/WWW/2000/01/sw/swad-chart#archive", "1", "1", "1", "1",
                                  "file:/home/cph/dig/scenario/transactions-cdk-v2.n3#agent", "2", "1", "1", "1",
                                  "file:/home/cph/dig/scenario/transactions-cdk-v2.n3#datetime", "4", "1", "1", "1" };
    final String basicClassStats = createSequenceFile(properties, classes,
      Analytics.getTailFields(BasicClassStats.class), new File(testOutput, "class"));
    final String basicPredicateStats = createSequenceFile(properties, predicates,
      Analytics.getTailFields(BasicPredicateStats.class), new File(testOutput, "predicate"));

    ud.compute(basicClassStats, testOutput.getAbsolutePath() + "/usefull-data-classes.out");
    ud.compute(basicPredicateStats, testOutput.getAbsolutePath() + "/usefull-data-predicates.out");
  }

  @Test
  public void testExtra()
  throws Exception {
    //First generate useful-data.out
    testUsefulData();

    //Now run the test for classes
    final String table = "./src/main/resources/schema.org/sorted-table.txt";
    final ExtraSchema es = new ExtraSchema(table);
    es.compute(testOutput.getAbsolutePath() + "/usefull-data-classes.out",
      testOutput.getAbsolutePath() + "/extra-terms-classes.out");
    //Now run the test for predicates
    es.compute(testOutput.getAbsolutePath() + "/usefull-data-predicates.out",
      testOutput.getAbsolutePath() + "/extra-terms-predicates.out");
  }

}
