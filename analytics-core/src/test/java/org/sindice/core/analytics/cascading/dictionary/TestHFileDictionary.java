/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.dictionary;

import java.io.IOException;

import org.apache.hadoop.hbase.io.hfile.AnalyticsCacheConfig;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
import org.sindice.core.analytics.cascading.dictionary.DictionaryFactory.DictionaryType;
import org.sindice.core.analytics.testHelper.AbstractAnalyticsTestCase;
import org.sindice.core.analytics.testHelper.iotransformation.BytesType;
import org.sindice.core.analytics.testHelper.iotransformation.BytesWritableType;
import org.sindice.core.analytics.testHelper.iotransformation.IntType;
import org.sindice.core.analytics.testHelper.iotransformation.LongType;
import org.sindice.core.analytics.testHelper.iotransformation.StringType;
import org.sindice.core.analytics.testHelper.iotransformation.TupleType;
import org.sindice.core.analytics.testHelper.iotransformation.UnitOfWorkTestHelper;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.tuple.Tuple;

public class TestHFileDictionary extends AbstractAnalyticsTestCase {

  @Override
  @Before
  public void setUp()
  throws Exception {
    super.setUp();
    properties.setProperty(AnalyticsParameters.DICTIONARY.toString(), DictionaryType.HFILE.toString());
  }

  @Override
  public void tearDown() throws Exception {
    AnalyticsCacheConfig.unRegisterAll();
    super.tearDown();
  }

  /**
   * TODO: This test shows that in presence of entries with the same key, the returned valued when that key is searched
   * is the first entry. There should be a mechanism for checking this and throwing an Exception.
   */
  @Test
  @Ignore
  public void testDuplicateKeys() throws IOException {
    final String input = "src/test/resources/testHFileDictionary/testDuplicateKeys/input.check";
    final Class[] types = { LongType.class, StringType.class };

    properties.setProperty(AnalyticsParameters.HFILE_PREFIX.toString(), "testDuplicateKeys");
    final String hfile = UnitOfWorkTestHelper.createHFile(properties, types, input, testOutput);

    properties.setProperty("testDuplicateKeys", hfile);
    final FlowProcess fp = new HadoopFlowProcess(HadoopUtil.createJobConf(properties, new JobConf()));
    final Dictionary dict = DictionaryFactory.getDictionary(fp, "testDuplicateKeys");

    try {
      final String n3 = (String) dict.getValue(10l);
      assertTrue(n3 != null);
      assertEquals("pierre", n3);
    } finally {
      dict.close();
    }
  }

  @Test
  public void testLongKeys() throws IOException {
    final String input = "src/test/resources/testHFileDictionary/testLongKeys/input.check";
    final Class[] types = { LongType.class, StringType.class };

    properties.setProperty(AnalyticsParameters.HFILE_PREFIX.toString(), "testLongKeys");
    final String hfile = UnitOfWorkTestHelper.createHFile(properties, types, input, testOutput);

    properties.setProperty("testLongKeys", hfile);
    final FlowProcess fp = new HadoopFlowProcess(HadoopUtil.createJobConf(properties, new JobConf()));
    final Dictionary dict = DictionaryFactory.getDictionary(fp, "testLongKeys");

    try {
      final String n3 = (String) dict.getValue(10L);
      assertEquals("pierre", n3);
      final String n1 = (String) dict.getValue(-10L);
      assertEquals("jean", n1);
      final String n60 = (String) dict.getValue(3L);
      assertEquals("curie", n60);
    } finally {
      dict.close();
    }
  }

  @Test
  public void testLongToTuple() throws IOException {
    final String input = "src/test/resources/testHFileDictionary/testLongToTuple/input.check";
    final Class[] types = { LongType.class, TupleType.class };

    properties.setProperty(TupleType.FIELD_TYPE, IntType.class.getName());
    properties.setProperty(AnalyticsParameters.HFILE_PREFIX.toString(), "testLongToTuple");
    final String hfile = UnitOfWorkTestHelper.createHFile(properties, types, input, testOutput);

    properties.setProperty("testLongToTuple", hfile);
    final FlowProcess fp = new HadoopFlowProcess(HadoopUtil.createJobConf(properties, new JobConf()));
    final Dictionary dict = DictionaryFactory.getDictionary(fp, "testLongToTuple");

    try {
      final Tuple n10 = (Tuple) dict.getValue(10L);
      assertEquals(new Tuple(-10), n10);
      final Tuple n10Dark = (Tuple) dict.getValue(-10L);
      assertEquals(new Tuple(10), n10Dark);
    } finally {
      dict.close();
    }
  }

  @Test
  public void testLongToInt() throws IOException {
    final String input = "src/test/resources/testHFileDictionary/testLongToInt/input.check";
    final Class[] types = { LongType.class, IntType.class };

    properties.setProperty(AnalyticsParameters.HFILE_PREFIX.toString(), "testLongToInt");
    final String hfile = UnitOfWorkTestHelper.createHFile(properties, types, input, testOutput);

    properties.setProperty("testLongToInt", hfile);
    final FlowProcess fp = new HadoopFlowProcess(HadoopUtil.createJobConf(properties, new JobConf()));
    final Dictionary dict = DictionaryFactory.getDictionary(fp, "testLongToInt");

    try {
      final Integer n3 = (Integer) dict.getValue(10L);
      assertTrue(n3 != null);
      assertEquals(33, n3.intValue());
      final Integer n1 = (Integer) dict.getValue(-10L);
      assertTrue(n1 != null);
      assertEquals(22, n1.intValue());
      final Integer n60 = (Integer) dict.getValue(3L);
      assertTrue(n60 != null);
      assertEquals(66, n60.intValue());
    } finally {
      dict.close();
    }
  }

  @Test
  public void testOneEntry()
  throws IOException {
    final String input = "src/test/resources/testHFileDictionary/testOneEntry/input.check";
    final Class[] types = { LongType.class, StringType.class };

    properties.setProperty(AnalyticsParameters.HFILE_PREFIX.toString(), "testOneEntry");
    final String hfile = UnitOfWorkTestHelper.createHFile(properties, types, input, testOutput);

    properties.setProperty("testOneEntry", hfile);
    final FlowProcess fp = new HadoopFlowProcess(HadoopUtil.createJobConf(properties, new JobConf()));
    final Dictionary dict = DictionaryFactory.getDictionary(fp, "testOneEntry");

    try {
      final String n1 = (String) dict.getValue(-10L);
      assertEquals("jean", n1);

      final String n2 = (String) dict.getValue(-10L);
      assertEquals("jean", n2);
    } finally {
      dict.close();
    }
  }

  @Test
  public void testLongToString()
  throws IOException {
    final String input = "src/test/resources/testHFileDictionary/testLongToString/input.check";
    final Class[] types = { LongType.class, StringType.class };

    properties.setProperty(AnalyticsParameters.HFILE_PREFIX.toString(), "testLongToString");
    final String hfile = UnitOfWorkTestHelper.createHFile(properties, types, input, testOutput);

    properties.setProperty("testLongToString", hfile);
    final FlowProcess fp = new HadoopFlowProcess(HadoopUtil.createJobConf(properties, new JobConf()));
    final Dictionary dict = DictionaryFactory.getDictionary(fp, "testLongToString");

    try {
      final String n3 = (String) dict.getValue(10L);
      assertEquals("pierre", n3);
      final String n1 = (String) dict.getValue(-10L);
      assertEquals("jean", n1);
      final String n60 = (String) dict.getValue(3L);
      assertEquals("curie", n60);
    } finally {
      dict.close();
    }
  }

  @Test
  public void testStringToString() throws IOException {
    final String input = "src/test/resources/testHFileDictionary/testStringToString/input.check";
    final Class[] types = { StringType.class, StringType.class };

    properties.setProperty(AnalyticsParameters.HFILE_PREFIX.toString(), "testStringToString");
    final String hfile = UnitOfWorkTestHelper.createHFile(properties, types, input, testOutput);

    properties.setProperty("testStringToString", hfile);
    final FlowProcess fp = new HadoopFlowProcess(HadoopUtil.createJobConf(properties, new JobConf()));
    final Dictionary dict = DictionaryFactory.getDictionary(fp, "testStringToString");

    try {
      final String n3 = (String) dict.getValue("name3");
      assertEquals("curie", n3);
      final String n1 = (String) dict.getValue("name1");
      assertEquals("marie", n1);
      final String n60 = (String) dict.getValue("name60");
      assertEquals("ludo", n60);
      assertTrue(dict.getValue("name22222") == null);
    } finally {
      dict.close();
    }
  }

  @Test
  public void testByteArrayToString() throws IOException {
    final String input = "src/test/resources/testHFileDictionary/testByteArrayToString/input.check";
    final Class[] types = { BytesType.class, StringType.class };

    properties.setProperty(AnalyticsParameters.HFILE_PREFIX.toString(), "testByteArrayToString");
    final String hfile = UnitOfWorkTestHelper.createHFile(properties, types, input, testOutput);

    properties.setProperty("testByteArrayToString", hfile);
    final FlowProcess fp = new HadoopFlowProcess(HadoopUtil.createJobConf(properties, new JobConf()));
    final Dictionary dict = DictionaryFactory.getDictionary(fp, "testByteArrayToString");

    try {
      final String n3 = (String) dict.getValue(new byte[] { 42 });
      assertEquals("one", n3);
      final String n1 = (String) dict.getValue(new byte[] { 1, 9, 8, 7 });
      assertEquals("three", n1);
      final String n60 = (String) dict.getValue(new byte[] { 24, 45, 19 });
      assertEquals("two", n60);
    } finally {
      dict.close();
    }
  }

  @Test
  public void testBytesWritableToString() throws IOException {
    final String input = "src/test/resources/testHFileDictionary/testBytesWritableToString/input.check";
    final Class[] types = { BytesWritableType.class, StringType.class };

    properties.setProperty(AnalyticsParameters.HFILE_PREFIX.toString(), "testBytesWritableToString");
    final String hfile = UnitOfWorkTestHelper.createHFile(properties, types, input, testOutput);

    properties.setProperty("testBytesWritableToString", hfile);
    final FlowProcess fp = new HadoopFlowProcess(HadoopUtil.createJobConf(properties, new JobConf()));
    final Dictionary dict = DictionaryFactory.getDictionary(fp, "testBytesWritableToString");

    try {
      final String n3 = (String) dict.getValue(new BytesWritable(new byte[] { 42 }));
      assertEquals("one", n3);
      final String n1 = (String) dict.getValue(new BytesWritable(new byte[] { 1, 9, 8, 7 }));
      assertEquals("three", n1);
      final String n60 = (String) dict.getValue(new BytesWritable(new byte[] { 24, 45, 19 }));
      assertEquals("two", n60);
    } finally {
      dict.close();
    }
  }

}
