/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster;


import org.junit.Test;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.testHelper.iotransformation.UnitOfWorkTestHelper;
import org.sindice.graphsummary.AbstractSummaryTestCase;
import org.sindice.graphsummary.cascading.ecluster.singletype.SingleTypeClusterSubAssembly;
import org.sindice.graphsummary.iotransformation.EntityDescriptionType;
import org.sindice.graphsummary.iotransformation.Hash128Type;
import org.sindice.graphsummary.iotransformation.Hash64Type;
import org.sindice.graphsummary.iotransformation.SortedListToHash128Type;

/**
 * 
 */
public class TestSingleTypeClustering
extends AbstractSummaryTestCase {

  @Test
  public void testGetCIDSingleType()
  throws Exception {
    final String input = RES + "testSingleTypeClustering/testGetCIDSingleType/input.check";
    final String output = RES + "testSingleTypeClustering/testGetCIDSingleType/output.check";

    Class<?>[] typesInput = { Hash64Type.class, Hash128Type.class, EntityDescriptionType.class,
        EntityDescriptionType.class };
    Class<?>[] typesOutput = { Hash64Type.class, Hash128Type.class, EntityDescriptionType.class,
        EntityDescriptionType.class, SortedListToHash128Type.class };

    UnitOfWorkTestHelper.addSource(Analytics.getName(SingleTypeClusterSubAssembly.class), input, typesInput);
    UnitOfWorkTestHelper.addSink(Analytics.getName(SingleTypeClusterSubAssembly.class), output, typesOutput);
    UnitOfWorkTestHelper.runTestAssembly(SingleTypeClusterSubAssembly.class, testOutput, properties);
  }

}
