/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster;


import org.junit.Test;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.testHelper.iotransformation.UnitOfWorkTestHelper;
import org.sindice.graphsummary.AbstractSummaryTestCase;
import org.sindice.graphsummary.cascading.ecluster.types.TypesClusterSubAssembly;
import org.sindice.graphsummary.iotransformation.EntityDescriptionType;
import org.sindice.graphsummary.iotransformation.Hash128Type;
import org.sindice.graphsummary.iotransformation.Hash64Type;
import org.sindice.graphsummary.iotransformation.SortedListToHash128Type;

/**
 * 
 */
public class TestTypesClustering
extends AbstractSummaryTestCase {


  @Test
  public void testEnableCheckAuthType()
  throws Exception {
    properties.setProperty(AnalyticsParameters.CHECK_AUTH_TYPE.toString(), "true");
    run("testEnableCheckAuthType");
  }

  @Test
  public void testDisableCheckAuthType()
  throws Exception {
    properties.setProperty(AnalyticsParameters.CHECK_AUTH_TYPE.toString(), "false");
    run("testDisableCheckAuthType");
  }

  @Test
  public void testGetCIDTypes()
  throws Exception {
    run("testGetCIDTypes");
  }

  @Test
  public void testEmptyTypes()
  throws Exception {
    run("testEmptyTypes");
  }

  @Test
  public void testPropertiesGraph()
  throws Exception {
    run("testPropertiesGraph");
  }

  @Test
  public void testDuplicateTypes()
  throws Exception {
    run("testDuplicateTypes");
  }

  @Test
  public void testEmptyProperties()
  throws Exception {
    run("testEmptyProperties");
  }

  /**
   * Execute the {@link TypesClusterSubAssembly}
   */
  private void run(String folder)
  throws Exception {
    final String input = "./src/test/resources/testTypesClustering/" + folder + "/input.check";
    final String output = "./src/test/resources/testTypesClustering/" + folder + "/output.check";

    Class<?>[] typesInput = { Hash64Type.class, Hash128Type.class, EntityDescriptionType.class, EntityDescriptionType.class };
    Class<?>[] typesOutput = { Hash64Type.class, Hash128Type.class, EntityDescriptionType.class, EntityDescriptionType.class,
        SortedListToHash128Type.class };

    UnitOfWorkTestHelper.addSource(Analytics.getName(TypesClusterSubAssembly.class), input, typesInput);
    UnitOfWorkTestHelper.addSink(Analytics.getName(TypesClusterSubAssembly.class), output, typesOutput);
    UnitOfWorkTestHelper.runTestAssembly(TypesClusterSubAssembly.class, testOutput, properties);
  }

}
