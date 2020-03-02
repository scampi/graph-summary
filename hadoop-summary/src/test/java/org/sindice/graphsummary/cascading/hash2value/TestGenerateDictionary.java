/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.hash2value;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;
import org.sindice.analytics.entity.AnalyticsLiteral;
import org.sindice.analytics.entity.AnalyticsUri;
import org.sindice.analytics.entity.AnalyticsValue;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.dictionary.Dictionary;
import org.sindice.core.analytics.cascading.dictionary.DictionaryFactory;
import org.sindice.core.analytics.cascading.scheme.HFileScheme;
import org.sindice.core.analytics.rdf.DocumentFormat;
import org.sindice.core.analytics.util.Hash;
import org.sindice.graphsummary.AbstractSummaryTestCase;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

// TODO Update this test to use the other dictionary implementation (MapFileScheme)
public class TestGenerateDictionary extends AbstractSummaryTestCase {

  /**
   * GL-116
   */
  @Test
  public void testDatatypeDictionary()
  throws IOException {
    final String jsonPath = RES + "testGenerateDictionary/testDatatypeDictionary/input.nt";
    final Tap source = new Hfs(new TextLine(new Fields("value")), jsonPath);

    properties.setProperty(AnalyticsParameters.DOCUMENT_FORMAT.toString(), DocumentFormat.NTRIPLES.toString());
    final Map<String, Tap> sinks = new HashMap<String, Tap>();
    sinks.put("type", new Hfs(new HFileScheme(new Fields("hash", "string"), GenerateDictionary.CLASS_PREFIX),
      new File(testOutput, "testT").getPath()));
    sinks.put("predicate", new Hfs(new HFileScheme(new Fields("hash", "string"), GenerateDictionary.PREDICATE_PREFIX),
      new File(testOutput, "testP").getPath()));
    sinks.put("domain", new Hfs(new HFileScheme(new Fields("hash", "string"), GenerateDictionary.DOMAIN_PREFIX),
      new File(testOutput, "testD").getPath()));
    sinks.put("datatype", new Hfs(new HFileScheme(new Fields("hash", "string"), GenerateDictionary.DATATYPE_PREFIX),
      new File(testOutput, "testDT").getPath()));

    FlowConnector flowConnector = new HadoopFlowConnector(properties);
    Flow flow = flowConnector.connect(Analytics.getName(GenerateDictionary.class),
      source, sinks, new GenerateDictionary());
    flow.complete();

    checkDictionary(sinks.get("datatype").getIdentifier(), GenerateDictionary.DATATYPE_PREFIX,
      new AnalyticsUri("http://date.com"));
  }

  @Test
  public void testDictionaryComputation2() throws IOException {
    final String jsonPath = RES + "testGenerateDictionary/testDictionaryComputation2/input.nt";
    final Tap source = new Hfs(new TextLine(new Fields("value")), jsonPath);

    properties.setProperty(AnalyticsParameters.DOCUMENT_FORMAT.toString(), DocumentFormat.NTRIPLES.toString());
    final Map<String, Tap> sinks = new HashMap<String, Tap>();
    sinks.put("type", new Hfs(new HFileScheme(new Fields("hash", "string"), GenerateDictionary.CLASS_PREFIX),
      new File(testOutput, "testT").getPath()));
    sinks.put("predicate", new Hfs(new HFileScheme(new Fields("hash", "string"), GenerateDictionary.PREDICATE_PREFIX),
      new File(testOutput, "testP").getPath()));
    sinks.put("domain", new Hfs(new HFileScheme(new Fields("hash", "string"), GenerateDictionary.DOMAIN_PREFIX),
      new File(testOutput, "testD").getPath()));
    sinks.put("datatype", new Hfs(new HFileScheme(new Fields("hash", "string"), GenerateDictionary.DATATYPE_PREFIX),
      new File(testOutput, "testDT").getPath()));

    FlowConnector flowConnector = new HadoopFlowConnector(properties);
    Flow flow = flowConnector.connect(Analytics.getName(GenerateDictionary.class),
      source, sinks, new GenerateDictionary());
    flow.complete();

    checkDictionary(sinks.get("type").getIdentifier(), GenerateDictionary.CLASS_PREFIX,
      new AnalyticsUri("http://rdf.freebase.com/ns/measurement_unit.dated_percentage"));
    checkDictionary(sinks.get("predicate").getIdentifier(), GenerateDictionary.PREDICATE_PREFIX,
      new AnalyticsUri("http://rdf.freebase.com/ns/type.property.unique"),
      new AnalyticsUri("http://rdf.freebase.com/ns/type.property.expected_type"),
      new AnalyticsUri("http://rdf.freebase.com/ns/type.object.name"));
  }

  /**
   * Test type literal without {@link AnalyticsParameters#NORM_LITERAL_TYPE normalization}
   */
  @Test
  public void testDictionaryLiteral() throws IOException {
    final String jsonPath = RES + "testGenerateDictionary/testDictionaryLiteral/input.json";
    final Tap source = new Hfs(new TextLine(new Fields("value")), jsonPath);

    final Map<String, Tap> sinks = new HashMap<String, Tap>();
    sinks.put("type", new Hfs(new HFileScheme(new Fields("hash", "string"), GenerateDictionary.CLASS_PREFIX),
      new File(testOutput, "testT").getPath()));
    sinks.put("predicate", new Hfs(new HFileScheme(new Fields("hash", "string"), GenerateDictionary.PREDICATE_PREFIX),
      new File(testOutput, "testP").getPath()));
    sinks.put("domain", new Hfs(new HFileScheme(new Fields("hash", "string"), GenerateDictionary.DOMAIN_PREFIX),
      new File(testOutput, "testD").getPath()));
    sinks.put("datatype", new Hfs(new HFileScheme(new Fields("hash", "string"), GenerateDictionary.DATATYPE_PREFIX),
      new File(testOutput, "testDT").getPath()));

    FlowConnector flowConnector = new HadoopFlowConnector(properties);
    Flow flow = flowConnector.connect(Analytics.getName(GenerateDictionary.class),
      source, sinks, new GenerateDictionary());
    flow.complete();

    checkDictionary(sinks.get("type").getIdentifier(), GenerateDictionary.CLASS_PREFIX,
      new AnalyticsLiteral("literal"));
  }

  @Test
  public void testDictionaryComputation() throws IOException {
    properties.setProperty(AnalyticsParameters.NORM_LITERAL_TYPE.toString(), "true");

    final String jsonPath = RES + "testGenerateDictionary/testDictionaryComputation/input.json";
    final Tap source = new Hfs(new TextLine(new Fields("value")), jsonPath);

    final Map<String, Tap> sinks = new HashMap<String, Tap>();
    sinks.put("type", new Hfs(new HFileScheme(new Fields("hash", "string"), GenerateDictionary.CLASS_PREFIX),
      new File(testOutput, "testT").getPath()));
    sinks.put("predicate", new Hfs(new HFileScheme(new Fields("hash", "string"), GenerateDictionary.PREDICATE_PREFIX),
      new File(testOutput, "testP").getPath()));
    sinks.put("domain", new Hfs(new HFileScheme(new Fields("hash", "string"), GenerateDictionary.DOMAIN_PREFIX),
      new File(testOutput, "testD").getPath()));
    sinks.put("datatype", new Hfs(new HFileScheme(new Fields("hash", "string"), GenerateDictionary.DATATYPE_PREFIX),
      new File(testOutput, "testDT").getPath()));

    FlowConnector flowConnector = new HadoopFlowConnector(properties);
    Flow flow = flowConnector.connect(Analytics.getName(GenerateDictionary.class),
      source, sinks, new GenerateDictionary());
    flow.complete();

    checkDictionary(sinks.get("type").getIdentifier(), GenerateDictionary.CLASS_PREFIX,
      new AnalyticsUri("http://www.domain1.com/~object1"), new AnalyticsUri("http://www.domain1.com/~object2"),
      new AnalyticsLiteral("literal"));
    checkDictionary(sinks.get("predicate").getIdentifier(), GenerateDictionary.PREDICATE_PREFIX,
      new AnalyticsUri("http://www.predicate/#p1"), new AnalyticsUri("http://www.predicate/#p2"),
      new AnalyticsUri("http://www.predicate/#p3"), new AnalyticsUri("http://www.predicate/#p4"));
    checkDictionary(sinks.get("domain").getIdentifier(), GenerateDictionary.DOMAIN_PREFIX,
      new AnalyticsLiteral("domain1.com"), new AnalyticsLiteral("domain2.com"));
  }

  /**
   * Assert the content of the created dictionary.
   * @param path the path to the dictionary
   * @param name the name of the HFile
   * @param expected the expected set of of values
   */
  private void checkDictionary(final String path, final String name, final AnalyticsValue...expected)
  throws IOException {
    final Path pathDict = new Path(path, name);
    final JobConf conf = HadoopUtil.createJobConf(properties, new JobConf());
    conf.set(name, pathDict.toUri().toString());
    final Dictionary dict = DictionaryFactory.getDictionary(new HadoopFlowProcess(conf), name);

    try {
      for (int i = 0; i < expected.length; ++i) {
        final long hash = Hash.getHash64(expected[i].getValue());
        assertEquals(expected[i], dict.getValue(hash));
      }
    } finally {
      dict.close();
    }
  }

}
