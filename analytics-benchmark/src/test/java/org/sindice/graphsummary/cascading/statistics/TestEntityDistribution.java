/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.statistics;


import org.junit.Test;
import org.sindice.analytics.benchmark.cascading.statistics.EntityDistribution;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.testHelper.iotransformation.UnitOfWorkTestHelper;
import org.sindice.graphsummary.AbstractSummaryTestCase;
import org.sindice.graphsummary.iotransformation.EntityDescriptionType;
import org.sindice.graphsummary.iotransformation.Hash128Type;
import org.sindice.graphsummary.iotransformation.Hash64Type;
import org.sindice.graphsummary.iotransformation.JsonType;

/**
 * 
 */
public class TestEntityDistribution
extends AbstractSummaryTestCase {

  @Test
  public void testEntityStats()
  throws Exception {
    final String input = "src/test/resources/testEntityDistribution/testEntityStats/input.check";
    final String output = "src/test/resources/testEntityDistribution/testEntityStats/output.check";

    Class<?>[] typesInput = { Hash64Type.class, Hash128Type.class,
        EntityDescriptionType.class, EntityDescriptionType.class };
    Class<?>[] typesOutput = { Hash64Type.class, Hash128Type.class, JsonType.class };

    UnitOfWorkTestHelper.addSource(Analytics.getName(EntityDistribution.class), input, typesInput);
    UnitOfWorkTestHelper.addSink(Analytics.getName(EntityDistribution.class), output, typesOutput);
    UnitOfWorkTestHelper.runTestAssembly(EntityDistribution.class, testOutput, properties);
  }

}
