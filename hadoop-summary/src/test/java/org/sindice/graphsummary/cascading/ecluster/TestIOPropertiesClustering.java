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
import org.sindice.graphsummary.cascading.ecluster.ioproperties.IOPropertiesClusterGeneratorSubAssembly;
import org.sindice.graphsummary.cascading.entity.GetClusterGraph;
import org.sindice.graphsummary.cascading.entity.GetClusterGraph.PreProcessing;
import org.sindice.graphsummary.iotransformation.EntityDescriptionType;
import org.sindice.graphsummary.iotransformation.Hash128Type;
import org.sindice.graphsummary.iotransformation.Hash64Type;
import org.sindice.graphsummary.iotransformation.SortedListToHash128Type;

/**
 * 
 */
public class TestIOPropertiesClustering
extends AbstractSummaryTestCase {

  @Test
  public void testGetCIDIOPaths()
  throws Exception {
    run("testGetCIDIOPaths", false);
  }

  @Test
  public void testGetCIDIOPathsClasses()
  throws Exception {
    run("testGetCIDIOPathsClasses", true);
  }

  @Test
  public void testEmptyProperties()
  throws Exception {
    run("testEmptyProperties", false);
  }

  /**
   * Run the {@link IOPropertiesClusterGeneratorSubAssembly}.
   */
  private void run(String folder, boolean withClass) throws Exception {
    final String input = "./src/test/resources/testIOPropertiesClustering/" + folder + "/input.check";
    final String output1 = "./src/test/resources/testIOPropertiesClustering/" + folder + "/output1.check";
    final String output2 = "./src/test/resources/testIOPropertiesClustering/" + folder + "/output2.check";

    Class<?>[] typesInput = { StringType.class };
    Class<?>[] typesOutput1 = { Hash64Type.class, Hash128Type.class,
        EntityDescriptionType.class, EntityDescriptionType.class };
    Class<?>[] typesOutput2 = { Hash64Type.class, Hash128Type.class,
        EntityDescriptionType.class, EntityDescriptionType.class, SortedListToHash128Type.class };

    properties.setProperty(AnalyticsParameters.DOCUMENT_FORMAT.toString(), DocumentFormat.NTRIPLES.toString());
    properties.setProperty(AnalyticsParameters.DEFAULT_DOMAIN.toString(), "domain1.com");

    UnitOfWorkTestHelper.addSource(Analytics.getName(GetClusterGraph.class), input, typesInput);
    UnitOfWorkTestHelper.addSink(Analytics.getName(GetClusterGraph.class), output1, typesOutput1);
    UnitOfWorkTestHelper.runTestAssembly(GetClusterGraph.class, testOutput, properties,
      PreProcessing.IO_EDGES_AGGREGATE);

    UnitOfWorkTestHelper.reset();
    UnitOfWorkTestHelper
      .addSource(Analytics.getName(IOPropertiesClusterGeneratorSubAssembly.class), output1, typesOutput1);
    UnitOfWorkTestHelper
      .addSink(Analytics.getName(IOPropertiesClusterGeneratorSubAssembly.class), output2, typesOutput2);
    UnitOfWorkTestHelper.runTestAssembly(IOPropertiesClusterGeneratorSubAssembly.class,
      testOutput, properties, withClass);
  }

}
