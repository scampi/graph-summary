/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.precision;

import org.junit.Test;
import org.sindice.analytics.benchmark.cascading.precision.connectivity.ConnectivityAssembly;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.testHelper.iotransformation.LongType;
import org.sindice.core.analytics.testHelper.iotransformation.TupleType;
import org.sindice.core.analytics.testHelper.iotransformation.UnitOfWorkTestHelper;
import org.sindice.graphsummary.AbstractSummaryTestCase;
import org.sindice.graphsummary.iotransformation.Hash64Type;
import org.sindice.graphsummary.iotransformation.SortedListToHash128Type;

public class TestConnectivityAssembly extends AbstractSummaryTestCase {

  @Test
  public void testTFPositive1()
  throws Exception {
    runConnectivityAssembly("testTFPositive1");
  }

  /**
   * Test for blank equivalence class
   */
  @Test
  public void testTFPositive2()
  throws Exception {
    runConnectivityAssembly("testTFPositive2");
  }

  /**
   * Test for blank equivalence class occurring as a source, connecting two non-blank clusters.
   */
  @Test
  public void testTFPositive3()
  throws Exception {
    runConnectivityAssembly("testTFPositive3");
  }

  /**
   * Test for disjoints: island + no outgoing link gold clusters
   */
  @Test
  public void testTFPositive4()
  throws Exception {
    runConnectivityAssembly("testTFPositive4");
  }

  /**
   * Runs the {@link ConnectivityAssembly} over the folder's data.
   * @param folder the test name
   */
  private void runConnectivityAssembly(final String folder)
  throws Exception {
    final String evalLinksetPath = RES + "testPrecision/testConnectivityAssembly/" + folder + "/eval-linkset.check";
    final String tfPosPath = RES + "testPrecision/testConnectivityAssembly/" + folder + "/tf-positive.check";

    Class<?>[] evalLinksetTypes = { Hash64Type.class, SortedListToHash128Type.class, LongType.class,
        SortedListToHash128Type.class, LongType.class, TupleType.class, SortedListToHash128Type.class, LongType.class,
        SortedListToHash128Type.class, LongType.class, TupleType.class };
    Class<?>[] tfPosTypes = { Hash64Type.class, SortedListToHash128Type.class, SortedListToHash128Type.class,
        LongType.class, LongType.class, LongType.class };

    properties.setProperty(TupleType.FIELD_TYPE, Hash64Type.class.getName());

    UnitOfWorkTestHelper.addSource(Analytics.getName(ConnectivityAssembly.class), evalLinksetPath, evalLinksetTypes);
    UnitOfWorkTestHelper.addSink(Analytics.getName(ConnectivityAssembly.class), tfPosPath, tfPosTypes);
    UnitOfWorkTestHelper.runTestAssembly(ConnectivityAssembly.class, testOutput, properties);
  }

}
