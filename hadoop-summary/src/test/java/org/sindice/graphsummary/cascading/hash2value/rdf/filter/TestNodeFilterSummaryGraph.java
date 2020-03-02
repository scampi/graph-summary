/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.hash2value.rdf.filter;

import org.junit.Test;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.testHelper.iotransformation.UnitOfWorkTestHelper;
import org.sindice.core.analytics.testHelper.iotransformation.StringType;
import org.sindice.graphsummary.AbstractSummaryTestCase;
import org.sindice.graphsummary.cascading.hash2value.rdf.DataGraphSummaryVocab;

public class TestNodeFilterSummaryGraph
extends AbstractSummaryTestCase {

  @Test
  public void testNodeFilterCardinality()
  throws Exception {
    final String filterQuery = "?node <" + DataGraphSummaryVocab.DOMAIN_URI + "> ?d ;" +
                               "<" + DataGraphSummaryVocab.CARDINALITY + "> ?card ." +
                               "FILTER(?card = 1)";

    final String input = "./src/test/resources/testNodeFilterSummaryGraph/testNodeFilterCardinality/input.txt";
    final String output = "./src/test/resources/testNodeFilterSummaryGraph/testNodeFilterCardinality/output.txt";

    Class<?>[] typesInput = { StringType.class };
    Class<?>[] typesOutput = { StringType.class };

    UnitOfWorkTestHelper.addSource(Analytics.getName(NodeFilterSummaryGraph.class), input, typesInput);
    UnitOfWorkTestHelper.addSink(Analytics.getName(NodeFilterSummaryGraph.class), output, typesOutput);
    UnitOfWorkTestHelper.runTestAssembly(NodeFilterSummaryGraph.class, testOutput, properties, filterQuery);
  }

  @Test
  public void testAlphaNumNodeUri()
  throws Exception {
    final String filterQuery = "?node <" + DataGraphSummaryVocab.DOMAIN_URI + "> ?d ;" +
                               "<" + DataGraphSummaryVocab.CARDINALITY + "> ?card ." +
                               "FILTER(?card = 1)";

    final String input = "./src/test/resources/testNodeFilterSummaryGraph/testAlphaNumNodeUri/input.txt";
    final String output = "./src/test/resources/testNodeFilterSummaryGraph/testAlphaNumNodeUri/output.txt";

    Class<?>[] typesInput = { StringType.class };
    Class<?>[] typesOutput = { StringType.class };

    UnitOfWorkTestHelper.addSource(Analytics.getName(NodeFilterSummaryGraph.class), input, typesInput);
    UnitOfWorkTestHelper.addSink(Analytics.getName(NodeFilterSummaryGraph.class), output, typesOutput);
    UnitOfWorkTestHelper.runTestAssembly(NodeFilterSummaryGraph.class, testOutput, properties, filterQuery);
  }

}
