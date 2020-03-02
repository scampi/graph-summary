/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.stats.basic;

import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.sindice.core.analytics.stats.basic.rdf.CollectionAnalyticsVocab;
import org.sindice.core.analytics.testHelper.AbstractAnalyticsTestCase;

import cascading.cascade.Cascade;
import cascading.flow.Flow;
import cascading.scheme.hadoop.TextLine;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

public class BasicStatsCascadeTest
extends AbstractAnalyticsTestCase {

  protected final static String jsonPath    = "./src/test/resources/basic-input.json";
  protected final static String obj1        = "http://www.domain1.com/~object1";
  protected final static String obj2        = "http://www.domain1.com/~object2";
  protected final static int    totalTuples = 2;

  @Test
  public void testClassCascade()
  throws IOException {
    final Properties rdfProps = new Properties();
    rdfProps.putAll(properties);
    rdfProps.setProperty("date", "2000-00-00");
    final Properties topkProps = new Properties();
    topkProps.putAll(properties);
    topkProps.setProperty(BasicStatsCascade.FILTER_THRESHOLD, "0");
    topkProps.setProperty(BasicStatsCascade.FILTER_FIELDS, "5");
    topkProps.setProperty(BasicStatsCascade.SORT_FIELDS, "5");
    final HashMap<String, Properties> p = new HashMap<String, Properties>();
    p.put(BasicStatsCascade.STAT_FLOW, properties);
    p.put(BasicStatsCascade.TOPK_FLOW, topkProps);
    p.put(BasicStatsCascade.RDF_FLOW, rdfProps);
    final Cascade cascade = BasicStatsCascade.getClassCascade(jsonPath,
      new TextLine(new Fields("value")), testOutput.getAbsolutePath(), p);

    cascade.complete();

    final ArrayList<String> expectedTuples = new ArrayList<String>();
    expectedTuples.add(obj1 + "\t2\t2\t2\t1");
    expectedTuples.add(obj2 + "\t5\t3\t3\t2");
    /*
     * Test MAP sink
     */
    final String mapPath = "file:" + new Path(testOutput.getAbsolutePath(), "MAP");
    assertFalse(cascade.findFlowsSinkingTo(mapPath).isEmpty());
    assertEquals(1, cascade.findFlowsSinkingTo(mapPath).size());
    TupleEntryIterator itMap = cascade.findFlowsSinkingTo(mapPath).toArray(new Flow[0])[0].openSink();

    int nTuple = 0;
    try {
      while (itMap.hasNext()) {
        TupleEntry tuple = itMap.next();
        assertEquals(tuple.getTuple().toString(), expectedTuples.get(nTuple));
        nTuple++;
      }
    } finally {
      itMap.close();
    }
    // Check the correct number of tuples
    assertEquals(totalTuples, nTuple);
    /*
     * Test TEXT sink
     */
    final ArrayList<String> csvlines = new ArrayList<String>();
    final String textPath = "file:" + new Path(testOutput.getAbsolutePath(), "TEXT");
    assertFalse(cascade.findFlowsSinkingTo(textPath).isEmpty());
    assertEquals(1, cascade.findFlowsSinkingTo(textPath).size());
    TupleEntryIterator itText = cascade.findFlowsSinkingTo(textPath).toArray(new Flow[0])[0].openSink();
    try {
      while (itText.hasNext()) {
        TupleEntry tuple = itText.next();
        csvlines.add(tuple.getString("line"));
      }
    } finally {
      itText.close();
    }
    Collections.sort(expectedTuples);
    Collections.sort(csvlines);
    assertArrayEquals(expectedTuples.toArray(new String[0]), csvlines.toArray(new String[0]));
    /*
     * Test RDF sink
     */
    final String rdfPath = "file:" + new Path(testOutput.getAbsolutePath(), "RDF");
    assertFalse(cascade.findFlowsSinkingTo(rdfPath).isEmpty());
    assertEquals(1, cascade.findFlowsSinkingTo(rdfPath).size());
    TupleEntryIterator itRDF = cascade.findFlowsSinkingTo(rdfPath).toArray(new Flow[0])[0].openSink();
    final ArrayList<String> data = new ArrayList<String>();
    try {
      while (itRDF.hasNext()) {
        TupleEntry tuple = itRDF.next();
        data.add(tuple.getString("line"));
      }
    } finally {
      itRDF.close();
    }
    assertTrue(RDFHelper.contains(data, CollectionAnalyticsVocab.STATISTIC_NAME, CollectionAnalyticsVocab.COL_CLASS_STAT));
    assertTrue(RDFHelper.contains(data, CollectionAnalyticsVocab.LABEL, obj1));
    assertTrue(RDFHelper.contains(data, CollectionAnalyticsVocab.LABEL, obj2));
    assertTrue(RDFHelper.contains(data, CollectionAnalyticsVocab.N_DOMAINS, "2"));
    assertTrue(RDFHelper.contains(data, CollectionAnalyticsVocab.N_REFS, "5"));
  }

}
