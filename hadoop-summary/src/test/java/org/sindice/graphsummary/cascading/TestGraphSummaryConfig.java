/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sindice.graphsummary.cascading.ecluster.ClusteringFactory.ClusteringAlgorithm;
import org.sindice.graphsummary.cascading.entity.GetClusterGraph.PreProcessing;

/**
 * 
 */
public class TestGraphSummaryConfig {

  @Before
  public void setUp() {
    GraphSummaryConfig.setConfFile("./src/test/resources/testGraphSummaryConfig/graph-summary_test.yaml");
  }

  @After
  public void tearDown() {
    GraphSummaryConfig.reset();
  }

  @Test
  public void testExistingConfig()
  throws IOException {
    final String key = ClusteringAlgorithm.class.getSimpleName();
    final String value = ClusteringAlgorithm.IO_PROPERTIES.toString();
    final Map<String, String> conf = GraphSummaryConfig.get(key, value);

    assertNotNull(conf);
    assertEquals(PreProcessing.IO_EDGES_AGGREGATE.toString(),
      conf.get(PreProcessing.class.getSimpleName()));
  }

  @Test
  public void testNonExistingConfig()
  throws IOException {
    final String key = ClusteringAlgorithm.class.getSimpleName();
    final Map<String, String> conf = GraphSummaryConfig.get(key, "boo");
    assertNotNull(conf);
    assertEquals("default", conf.get(GraphSummaryConfig.CONF_NAME));
  }

  @Test
  public void testNonSetProperty()
  throws IOException {
    GraphSummaryConfig.setConfFile("./src/test/resources/testGraphSummaryConfig/testNonSetProperty.yaml");
    final Map<String, String> conf = GraphSummaryConfig.get("key", "value");
    assertNotNull(conf);
    assertEquals(PreProcessing.O_EDGES_AGGREGATE.toString(), conf.get(PreProcessing.class.getSimpleName()));
  }

  /**
   * GL-98: Disable implicit type resolver in the YAML config file.
   */
  @Test
  public void testBoolean()
  throws IOException {
    GraphSummaryConfig.setConfFile("./src/test/resources/testGraphSummaryConfig/testBoolean.yaml");
    final Map<String, String> conf = GraphSummaryConfig.get(GraphSummaryConfig.CONF_NAME, GraphSummaryConfig.CONF_NAME_DEFAULT);
    assertNotNull(conf);
    assertEquals("true", conf.get("toto"));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testNonUniqueKeyValue()
  throws IOException {
    final String key = PreProcessing.class.getSimpleName();
    final String value = PreProcessing.IO_EDGES_AGGREGATE.toString();
    GraphSummaryConfig.get(key, value);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testMissingDefaultConf()
  throws IOException {
    GraphSummaryConfig.setConfFile("./src/test/resources/testGraphSummaryConfig/testMissingDefaultConf.yaml");

    final String key = GraphSummaryConfig.CONF_NAME;
    final String value = "boo";
    GraphSummaryConfig.get(key, value);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testDuplicateConfName()
  throws IOException {
    GraphSummaryConfig.setConfFile("./src/test/resources/testGraphSummaryConfig/testDuplicateConfName.yaml");

    final String key = GraphSummaryConfig.CONF_NAME;
    final String value = "boo";
    GraphSummaryConfig.get(key, value);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testDuplicateClusteringAlgorithm()
  throws IOException {
    GraphSummaryConfig.setConfFile("./src/test/resources/testGraphSummaryConfig/testDuplicateClusteringAlgorithm.yaml");

    final String key = ClusteringAlgorithm.class.getSimpleName();
    final String value = ClusteringAlgorithm.IO_PROPERTIES.toString();
    GraphSummaryConfig.get(key, value);
  }

}
