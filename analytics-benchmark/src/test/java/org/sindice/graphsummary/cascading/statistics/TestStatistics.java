/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.statistics;


import org.junit.Test;
import org.sindice.analytics.benchmark.cascading.statistics.EntityAverageDegree;
import org.sindice.analytics.benchmark.cascading.statistics.SummaryVolume;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.testHelper.iotransformation.LongType;
import org.sindice.core.analytics.testHelper.iotransformation.UnitOfWorkTestHelper;
import org.sindice.graphsummary.AbstractSummaryTestCase;
import org.sindice.graphsummary.iotransformation.DoubleType;
import org.sindice.graphsummary.iotransformation.EntityDescriptionType;
import org.sindice.graphsummary.iotransformation.Hash128Type;
import org.sindice.graphsummary.iotransformation.Hash64Type;
import org.sindice.graphsummary.iotransformation.PropertiesCountType;
import org.sindice.graphsummary.iotransformation.SortedListToHash128Type;
import org.sindice.graphsummary.iotransformation.TypesCountType;

/**
 * 
 */
public class TestStatistics
extends AbstractSummaryTestCase {

  @Test
  public void testSummarySizeOrder()
  throws Exception {
    final String propertiesPath = "src/test/resources/testStatistics/testSummarySizeOrder/properties.check";
    final String input = "src/test/resources/testStatistics/testSummarySizeOrder/input.check";

    Class<?>[] typesProperties = { Hash64Type.class, SortedListToHash128Type.class, TypesCountType.class,
      PropertiesCountType.class, LongType.class };
    Class<?>[] typesOutput = { Hash64Type.class, LongType.class, LongType.class };

    UnitOfWorkTestHelper.addSource(Analytics.getName(SummaryVolume.class), propertiesPath, typesProperties);
    UnitOfWorkTestHelper.addSink(Analytics.getName(SummaryVolume.class), input, typesOutput);
    UnitOfWorkTestHelper.runTestAssembly(SummaryVolume.class, testOutput, properties);
  }

  @Test
  public void testAverageDegree()
  throws Exception {
    final String entityPath = "src/test/resources/testStatistics/testAverageDegree/entity.check";
    final String avgOutdegree = "src/test/resources/testStatistics/testAverageDegree/avg-outdegree.check";
    final String avgIndegree = "src/test/resources/testStatistics/testAverageDegree/avg-indegree.check";

    Class<?>[] typesEntity = { Hash64Type.class, Hash128Type.class, EntityDescriptionType.class, EntityDescriptionType.class };
    Class<?>[] typesOutput = { Hash64Type.class, DoubleType.class };

    UnitOfWorkTestHelper.addSource(Analytics.getName(EntityAverageDegree.class), entityPath, typesEntity);
    UnitOfWorkTestHelper.addSink("avg-outdegree", avgOutdegree, typesOutput);
    UnitOfWorkTestHelper.addSink("avg-indegree", avgIndegree, typesOutput);
    UnitOfWorkTestHelper.runTestAssembly(EntityAverageDegree.class, testOutput, properties);
  }

}
