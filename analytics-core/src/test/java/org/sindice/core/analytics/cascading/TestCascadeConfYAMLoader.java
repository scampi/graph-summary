/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.junit.Test;


public class TestCascadeConfYAMLoader {

  @Test
  public void testSimpleConfiguration()
  throws Exception {
    final String config = "---\n" +
                          CascadeConfYAMLoader.DEFAULT_PARAMETERS + ":\n" +
                          "         - mapred.output.compression.codec: com.hadoop.compression.lzo.LzoCodec\n" +
                          "         - mapred.map.output.compression.codec: com.hadoop.compression.lzo.LzoCodec\n" +
                          "         - mapred.compress.map.output: true\n" +
                          "         - mapred.output.compress: true\n" +
                          "flowX:\n" +
                          "         - mapred.output.compression.codec: org.apache.hadoop.io.compress.GzipCodec\n" +
                          "...\n";
    final CascadeConfYAMLoader yaml = new CascadeConfYAMLoader();
    yaml.load(new ByteArrayInputStream(config.getBytes()));

    for (Entry<String, Properties> properties : yaml.getFlowsConfiguration("flowX").entrySet()) {
      if (properties.getKey().equals("flowX")) {
        assertTrue(properties.getValue().containsKey("mapred.output.compression.codec"));
        assertEquals("org.apache.hadoop.io.compress.GzipCodec", properties.getValue().get("mapred.output.compression.codec"));
        assertTrue(properties.getValue().containsKey("mapred.map.output.compression.codec"));
        assertEquals("com.hadoop.compression.lzo.LzoCodec", properties.getValue().get("mapred.map.output.compression.codec"));
        assertTrue(properties.getValue().containsKey("mapred.compress.map.output"));
        assertEquals("true", properties.getValue().get("mapred.compress.map.output"));
        assertTrue(properties.getValue().containsKey("mapred.output.compress"));
        assertEquals("true", properties.getValue().get("mapred.output.compress"));
      }
    }
  }

  @Test(expected=IllegalArgumentException.class)
  public void testWrongFlowname()
  throws Exception {
    final String config = "---\n" +
                          CascadeConfYAMLoader.DEFAULT_PARAMETERS + ":\n" +
                          "         - mapred.output.compression.codec: com.hadoop.compression.lzo.LzoCodec\n" +
                          "         - mapred.map.output.compression.codec: com.hadoop.compression.lzo.LzoCodec\n" +
                          "         - mapred.compress.map.output: true\n" +
                          "         - mapred.output.compress: true\n" +
                          "BAD_BAD_BAD:\n" +
                          "         - mapred.output.compression.codec: org.apache.hadoop.io.compress.GzipCodec\n" +
                          "...\n";
    final CascadeConfYAMLoader yaml = new CascadeConfYAMLoader();
    yaml.load(new ByteArrayInputStream(config.getBytes()));
    yaml.getFlowsConfiguration("flowX");
  }

  @Test(expected=ClassCastException.class)
  public void testWrongParameter()
  throws Exception {
    final String config = "---\n" +
                          CascadeConfYAMLoader.DEFAULT_PARAMETERS + ":\n" +
                          "         - mapred.output.compression.codec: com.hadoop.compression.lzo.LzoCodec\n" +
                          "         - mapred.map.output.compression.codec: com.hadoop.compression.lzo.LzoCodec\n" +
                          "         - mapred.compress.map.output\n" + // must be key/value
                          
                          "         - mapred.output.compress: true\n" +
                          "flowX:\n" +
                          "         - mapred.output.compression.codec: org.apache.hadoop.io.compress.GzipCodec\n" +
                          "...\n";
    final CascadeConfYAMLoader yaml = new CascadeConfYAMLoader();
    yaml.load(new ByteArrayInputStream(config.getBytes()));
  }

  @Test
  public void testAlreadyAddedProperties()
  throws Exception {
    final String config = "---\n" +
                          CascadeConfYAMLoader.DEFAULT_PARAMETERS + ":\n" +
                          "         - mapred.output.compression.codec: com.hadoop.compression.lzo.LzoCodec\n" +
                          "         - mapred.map.output.compression.codec: com.hadoop.compression.lzo.LzoCodec\n" +
                          "         - mapred.compress.map.output: true\n" +
                          "         - mapred.output.compress: true\n" +
                          "flowX:\n" +
                          "         - mapred.output.compression.codec: org.apache.hadoop.io.compress.GzipCodec\n" +
                          "         - mapred.output.compression.codec: com.hadoop.compression.lzo.LzoCodec\n" +
                          "...\n";
    final CascadeConfYAMLoader yaml = new CascadeConfYAMLoader();

    yaml.load(new ByteArrayInputStream(config.getBytes()));
    for (Entry<String, Properties> properties : yaml.getFlowsConfiguration("flowX").entrySet()) {
      if (properties.getKey().equals("flowX")) {
        assertTrue(properties.getValue().containsKey("mapred.output.compression.codec"));
        assertEquals("org.apache.hadoop.io.compress.GzipCodec,com.hadoop.compression.lzo.LzoCodec",
          properties.getValue().get("mapred.output.compression.codec"));
      } else {
        fail();
      }
    }
  }

  @Test
  public void testMultiValuedProperty()
  throws Exception {
    final String config = "---\n" +
                          CascadeConfYAMLoader.DEFAULT_PARAMETERS + ":\n" +
                          "         - mapred.output.compression.codec: com.hadoop.compression.lzo.LzoCodec\n" +
                          "         - mapred.map.output.compression.codec: com.hadoop.compression.lzo.LzoCodec\n" +
                          "         - mapred.compress.map.output: true\n" +
                          "         - mapred.output.compress: true\n" +
                          "flowX:\n" +
                          "         - mapred.output.compression.codec:\n" +
                          "                  - org.apache.hadoop.io.compress.GzipCodec\n" +
                          "                  - com.hadoop.compression.lzo.LzoCodec\n" +
                          "...\n";
    final CascadeConfYAMLoader yaml = new CascadeConfYAMLoader();

    yaml.load(new ByteArrayInputStream(config.getBytes()));
    for (Entry<String, Properties> properties : yaml.getFlowsConfiguration("flowX").entrySet()) {
      if (properties.getKey().equals("flowX")) {
        assertTrue(properties.getValue().containsKey("mapred.output.compression.codec"));
        assertEquals("org.apache.hadoop.io.compress.GzipCodec,com.hadoop.compression.lzo.LzoCodec",
          properties.getValue().get("mapred.output.compression.codec"));
      } else {
        fail();
      }
    }
  }

  @Test
  public void testCliParameters()
  throws Exception {
    final String config = "---\n" +
                          CascadeConfYAMLoader.DEFAULT_PARAMETERS + ":\n" +
                          "         - paramA: valueA\n" +
                          "flowX:\n" +
                          "         - paramB: valueB\n" +
                          "...\n";
    final CascadeConfYAMLoader yaml = new CascadeConfYAMLoader();
    final Configuration cliConf = new Configuration();

    cliConf.set("paramA", "A");
    cliConf.set("paramB", "B");
    cliConf.set("paramC", "C");
    yaml.load(cliConf, new ByteArrayInputStream(config.getBytes()));
    for (Entry<String, Properties> properties : yaml.getFlowsConfiguration("flowX").entrySet()) {
      if (properties.getKey().equals("flowX")) {
        assertTrue(properties.getValue().containsKey("paramA"));
        assertEquals("valueA", properties.getValue().get("paramA"));
        assertTrue(properties.getValue().containsKey("paramB"));
        assertEquals("valueB", properties.getValue().get("paramB"));
        assertTrue(properties.getValue().containsKey("paramC"));
        assertEquals("C", properties.getValue().get("paramC"));
      } else {
        fail();
      }
    }
  }

  /**
   * GL-100, changed in Gl-101
   */
  @Test
  public void testCliParameters2()
  throws Exception {
    final String config = "---\n" +
                          CascadeConfYAMLoader.DEFAULT_PARAMETERS + ":\n" +
                          "         - mapred.reduce.tasks: 23\n" +
                          "...\n";
    final String[] cli = "--files src".split(" ");
    final CascadeConfYAMLoader yaml = new CascadeConfYAMLoader();
    final Configuration cliConf = new Configuration();

    // GenericOptionParser from hadoop, --files option
    new GenericOptionsParser(cliConf, cli);

    yaml.load(cliConf, new ByteArrayInputStream(config.getBytes()));

    final Map<String, Properties> yamlConf = yaml.getFlowsConfiguration();
    assertEquals(1, yamlConf.size());
    for (Entry<String, Properties> properties : yamlConf.entrySet()) {
      assertTrue(properties.getValue().containsKey("mapred.reduce.tasks"));
      assertEquals("23", properties.getValue().get("mapred.reduce.tasks"));
    }
  }

  /**
   * Gl-101
   */
  @Test
  public void testCliParameters3()
  throws Exception {
    final String config = "---\n" +
                          CascadeConfYAMLoader.DEFAULT_PARAMETERS + ":\n" +
                          "         - mapred.reduce.tasks: 23\n" +
                          "...\n";
    final String[] cli = "-fs local -jt local".split(" ");
    final CascadeConfYAMLoader yaml = new CascadeConfYAMLoader();
    final Configuration cliConf = new Configuration();

    // GenericOptionParser from hadoop, --files option
    new GenericOptionsParser(cliConf, cli);

    yaml.load(cliConf, new ByteArrayInputStream(config.getBytes()));

    final Map<String, Properties> yamlConf = yaml.getFlowsConfiguration();
    assertEquals(1, yamlConf.size());
    for (Entry<String, Properties> properties : yamlConf.entrySet()) {
      assertTrue(properties.getValue().containsKey("mapred.reduce.tasks"));
      assertEquals("23", properties.getValue().get("mapred.reduce.tasks"));
    }
  }

}
