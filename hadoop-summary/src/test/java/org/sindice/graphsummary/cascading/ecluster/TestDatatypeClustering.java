/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster;


import org.junit.Before;
import org.junit.Test;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.testHelper.iotransformation.UnitOfWorkTestHelper;
import org.sindice.graphsummary.AbstractSummaryTestCase;
import org.sindice.graphsummary.cascading.ecluster.datatype.DatatypeClusterSubAssembly;
import org.sindice.graphsummary.iotransformation.EntityDescriptionType;
import org.sindice.graphsummary.iotransformation.Hash128Type;
import org.sindice.graphsummary.iotransformation.Hash64Type;
import org.sindice.graphsummary.iotransformation.SortedListToHash128Type;

/**
 * 
 */
public class TestDatatypeClustering
extends AbstractSummaryTestCase {

  @Override
  @Before
  public void setUp()
  throws Exception {
    super.setUp();
  }

  @Test
  public void testDatatypeCluster()
  throws Exception {
    run("testDatatypeCluster");
  }

  /**
   * GL-96
   */
  @Test
  public void testDatatypeCluster2()
  throws Exception {
    run("testDatatypeCluster");
  }

  @Test
  public void testMissingDatatype()
  throws Exception {
    run("testMissingDatatype");
  }

  /**
   * Run {@link DatatypeClusterSubAssembly}
   */
  private void run(String folder)
  throws Exception {
    final String input = "./src/test/resources/testDatatypeClustering/" + folder + "/input.check";
    final String output = "./src/test/resources/testDatatypeClustering/" + folder + "/output.check";

    Class<?>[] typesInput = { Hash64Type.class, Hash128Type.class,
        EntityDescriptionType.class, EntityDescriptionType.class };
    Class<?>[] typesOutput = { Hash64Type.class, Hash128Type.class,
        EntityDescriptionType.class, EntityDescriptionType.class, SortedListToHash128Type.class };

    UnitOfWorkTestHelper.addSource(Analytics.getName(DatatypeClusterSubAssembly.class), input, typesInput);
    UnitOfWorkTestHelper.addSink(Analytics.getName(DatatypeClusterSubAssembly.class), output, typesOutput);
    UnitOfWorkTestHelper.runTestAssembly(DatatypeClusterSubAssembly.class, testOutput, properties);
  }

}
