/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster;


import org.junit.Test;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.testHelper.iotransformation.UnitOfWorkTestHelper;
import org.sindice.graphsummary.AbstractSummaryTestCase;
import org.sindice.graphsummary.cascading.ecluster.properties.PropertiesClusterSubAssembly;
import org.sindice.graphsummary.iotransformation.EntityDescriptionType;
import org.sindice.graphsummary.iotransformation.Hash128Type;
import org.sindice.graphsummary.iotransformation.Hash64Type;
import org.sindice.graphsummary.iotransformation.SortedListToHash128Type;

/**
 * 
 */
public class TestPropertiesClustering
extends AbstractSummaryTestCase {

  @Test
  public void testGetCIDProperties()
  throws Exception {
    run("testGetCIDProperties");
  }

  @Test
  public void testEmptyTypes()
  throws Exception {
    run("testEmptyTypes");
  }

  @Test
  public void testEmptyProperties()
  throws Exception {
    run("testEmptyProperties");
  }

  /**
   * Run the {@link PropertiesClusterSubAssembly}
   */
  private void run(String folder)
  throws Exception {
    final String input = "./src/test/resources/testPropertiesClustering/" + folder + "/input.check";
    final String output = "./src/test/resources/testPropertiesClustering/" + folder + "/output.check";

    Class<?>[] typesInput = { Hash64Type.class, Hash128Type.class,
        EntityDescriptionType.class, EntityDescriptionType.class };
    Class<?>[] typesOutput = { Hash64Type.class, Hash128Type.class,
        EntityDescriptionType.class, EntityDescriptionType.class, SortedListToHash128Type.class };

    UnitOfWorkTestHelper.addSource(Analytics.getName(PropertiesClusterSubAssembly.class), input, typesInput);
    UnitOfWorkTestHelper.addSink(Analytics.getName(PropertiesClusterSubAssembly.class), output, typesOutput);
    UnitOfWorkTestHelper.runTestAssembly(PropertiesClusterSubAssembly.class, testOutput, properties);
  }

}
