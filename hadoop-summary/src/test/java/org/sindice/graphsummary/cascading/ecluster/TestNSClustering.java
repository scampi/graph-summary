/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster;


import org.junit.Test;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.rdf.DocumentFormat;
import org.sindice.core.analytics.testHelper.iotransformation.UnitOfWorkTestHelper;
import org.sindice.core.analytics.testHelper.iotransformation.StringType;
import org.sindice.graphsummary.AbstractSummaryTestCase;
import org.sindice.graphsummary.cascading.ecluster.namespace.NSClusterSubAssembly;
import org.sindice.graphsummary.cascading.entity.GetClusterGraph;
import org.sindice.graphsummary.cascading.entity.GetClusterGraph.PreProcessing;
import org.sindice.graphsummary.iotransformation.EntityDescriptionType;
import org.sindice.graphsummary.iotransformation.Hash128Type;
import org.sindice.graphsummary.iotransformation.Hash64Type;
import org.sindice.graphsummary.iotransformation.SortedListToHash128Type;

/**
 * 
 */
public class TestNSClustering
extends AbstractSummaryTestCase {

  @Test
  public void testNScluster()
  throws Exception {
    final String input = RES + "testNSClustering/testNScluster/input.check";
    final String output = RES + "testNSClustering/testNScluster/output.check";

    Class<?>[] typesInput = { Hash64Type.class, Hash128Type.class, EntityDescriptionType.class, EntityDescriptionType.class };
    Class<?>[] typesOutput = { Hash64Type.class, Hash128Type.class, EntityDescriptionType.class,
    EntityDescriptionType.class, SortedListToHash128Type.class };

    UnitOfWorkTestHelper.addSource(Analytics.getName(NSClusterSubAssembly.class), input, typesInput);
    UnitOfWorkTestHelper.addSink(Analytics.getName(NSClusterSubAssembly.class), output, typesOutput);
    UnitOfWorkTestHelper.runTestAssembly(NSClusterSubAssembly.class, testOutput, properties, false);
  }

  @Test
  public void testIONScluster()
  throws Exception {
    properties.setProperty(AnalyticsParameters.DOCUMENT_FORMAT.toString(), DocumentFormat.NTRIPLES.toString());
    properties.setProperty(AnalyticsParameters.DEFAULT_DOMAIN.toString(), "d.com");
    runIONScluster("testIONScluster");
  }

  @Test
  public void testIONSclusterBNode()
  throws Exception {
    properties.setProperty(AnalyticsParameters.DOCUMENT_FORMAT.toString(), DocumentFormat.SINDICE_EXPORT.toString());
    runIONScluster("testIONSclusterBNode");
  }

  /**
   * Run the {@link NSClusterSubAssembly}.
   */
  private void runIONScluster(final String folder)
  throws Exception {
    final String input = "./src/test/resources/testNSClustering/" + folder + "/input.check";
    final String output1 = "./src/test/resources/testNSClustering/" + folder + "/output1.check";
    final String output2 = "./src/test/resources/testNSClustering/" + folder + "/output2.check";

    Class<?>[] typesInput = { StringType.class };
    Class<?>[] typesOutput1 = { Hash64Type.class, Hash128Type.class,
        EntityDescriptionType.class, EntityDescriptionType.class };
    Class<?>[] typesOutput2 = { Hash64Type.class, Hash128Type.class,
        EntityDescriptionType.class, EntityDescriptionType.class, SortedListToHash128Type.class };

    UnitOfWorkTestHelper.addSource(Analytics.getName(GetClusterGraph.class), input, typesInput);
    UnitOfWorkTestHelper.addSink(Analytics.getName(GetClusterGraph.class), output1, typesOutput1);
    UnitOfWorkTestHelper.runTestAssembly(GetClusterGraph.class, testOutput, properties,
      PreProcessing.IO_OBJECT_EDGES_AGGREGATE);

    UnitOfWorkTestHelper.reset();
    UnitOfWorkTestHelper.addSource(Analytics.getName(NSClusterSubAssembly.class), output1, typesOutput1);
    UnitOfWorkTestHelper.addSink(Analytics.getName(NSClusterSubAssembly.class), output2, typesOutput2);
    UnitOfWorkTestHelper.runTestAssembly(NSClusterSubAssembly.class, testOutput, properties, true);
  }

}
