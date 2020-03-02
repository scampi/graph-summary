/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.properties;

import org.junit.Before;
import org.junit.Test;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.testHelper.iotransformation.LongType;
import org.sindice.core.analytics.testHelper.iotransformation.UnitOfWorkTestHelper;
import org.sindice.graphsummary.AbstractSummaryTestCase;
import org.sindice.graphsummary.cascading.SummaryParameters;
import org.sindice.graphsummary.cascading.properties.GetPropertiesGraph.PropertiesProcessing;
import org.sindice.graphsummary.iotransformation.EntityDescriptionType;
import org.sindice.graphsummary.iotransformation.Hash128Type;
import org.sindice.graphsummary.iotransformation.Hash64Type;
import org.sindice.graphsummary.iotransformation.PropertiesCountType;
import org.sindice.graphsummary.iotransformation.SortedListToHash128Type;
import org.sindice.graphsummary.iotransformation.TypesCountType;

/**
 * 
 */
public class TestGetPropertiesGraph
extends AbstractSummaryTestCase {

  @Override
  @Before
  public void setUp()
  throws Exception {
    super.setUp();
    properties.setProperty(AnalyticsParameters.CLASS_ATTRIBUTES_FIELD.toString(),
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type,"
        + "http://opengraphprotocol.org/schema/type,"
        + "http://opengraph.org/schema/type,"
        + "http://ogp.me/ns#type,"
        + "http://purl.org/dc/elements/1.1/type,"
        + "http://purl.org/stuff/rev#type,"
        + //Added 19 Oct 2011
        "http://purl.org/dc/terms/type,"
        + //Added 19 Oct 2011
        "http://dbpedia.org/property/type,"
        + "http://dbpedia.org/ontology/type,"
        + "http://dbpedia.org/ontology/Organisation/type,"
        + //Added 25 Oct 2011
        "http://xmlns.com/foaf/0.1/type" //Added 25 Oct 2011
    );
  }

  /**
   * Extracts the properties and types
   */
  @Test
  public void testGetClusterTypes()
  throws Exception {
    runGetPropertiesGraph("testGetClusterTypes");
  }

  @Test
  /**
   * Extracts the properties and types in the case of multiple different types
   */
  public void testPropertiesMultiTypes()
  throws Exception {
    runGetPropertiesGraph("testPropertiesMultiTypes");
  }

  @Test
  /**
   * Extracts the properties and types in the case of empty literals
   */
  public void testPropertiesEmptyLiteral()
  throws Exception {
    runGetPropertiesGraph("testPropertiesEmptyLitteral");
  }

  @Test
  /**
   * Extracts the properties and types in the case of empty literals
   */
  public void testMultipleClassAttributes()
  throws Exception {
    runGetPropertiesGraph("testMultipleClassAttributes");
  }

  @Test
  /**
   * Extracts the datatypes from literals
   */
  public void testDatatypes()
  throws Exception {
    properties.setProperty(SummaryParameters.DATATYPE.toString(), "true");
    runGetPropertiesGraph("testDatatypes");
  }

  /**
   * Run the {@link GetPropertiesGraph} over the data in test
   */
  private void runGetPropertiesGraph(String test)
  throws Exception {
    final String input = "./src/test/resources/testGetPropertiesGraph/" + test + "/input.check";
    final String output = "./src/test/resources/testGetPropertiesGraph/" + test + "/output.check";

    Class<?>[] typesInput = { Hash64Type.class, Hash128Type.class, EntityDescriptionType.class,
        EntityDescriptionType.class, SortedListToHash128Type.class };
    Class<?>[] typesOutput = { Hash64Type.class, SortedListToHash128Type.class, TypesCountType.class,
        PropertiesCountType.class, LongType.class };

    UnitOfWorkTestHelper.addSource(Analytics.getName(GetPropertiesGraph.class), input, typesInput);
    UnitOfWorkTestHelper.addSink(Analytics.getName(GetPropertiesGraph.class), output, typesOutput);
    UnitOfWorkTestHelper.runTestAssembly(GetPropertiesGraph.class, testOutput, properties,
      PropertiesProcessing.DEFAULT);
  }

}
