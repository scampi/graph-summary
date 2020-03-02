/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.testHelper;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;

/**
 *
 */
public class TestCheckScheme
extends AbstractAnalyticsTestCase {

  /**
   * GL-74
   */
  @Test
  public void testTypedField()
  throws Exception {
    assertCheckScheme(new Fields("f1", "f2"), "testTypedField",
      new Tuple(9, "ConTent of f2"), new Tuple("ConTent of f1", 9));
  }

  /**
   * GL-74
   */
  @Test
  public void testTypedFieldWithFlowProcess()
  throws Exception {
    properties.setProperty(TestTypedFieldWithFlowProcess.DATA, "grishka");
    assertCheckScheme(new Fields("f1"), "testTypedFieldWithFlowProcess", new Tuple("grishkaConTent of f1"));
  }

  @Test
  public void testOneTupleOneField()
  throws Exception {
    assertCheckScheme(new Fields("f1"), "testOneTupleOneField", new Tuple("content of f1"));
  }

  @Test
  public void testOneTupleTwoFields()
  throws Exception {
    assertCheckScheme(new Fields("f1", "f2"), "testOneTupleTwoFields", new Tuple("content of f1", "ConTent of f2"));
  }

  @Test
  public void testTwoTuplesOneField()
  throws Exception {
    assertCheckScheme(new Fields("f1"), "testTwoTuplesOneField",
      new Tuple("content of f1 - tuple 1"), new Tuple("content of f1 - tuple 2"));
  }

  @Test
  public void testTwoTuplesTwoFields()
  throws Exception {
    assertCheckScheme(new Fields("f1", "f2"), "testTwoTuplesTwoFields",
      new Tuple("content of f1 - tuple 1", "content of f2 - tuple 1"),
      new Tuple("content of f1 - tuple 2", "content of f2 - tuple 2"));
  }

  @Test
  public void testNullFields()
  throws Exception {
    assertCheckScheme(new Fields("f1", "f2", "f3"), "testNullFields",
      new Tuple("c1", null, "c3"), new Tuple(null, "c2", "c3"), new Tuple("c1", "c2", null));
  }

  @Test
  public void testEmptyFields()
  throws Exception {
    assertCheckScheme(new Fields("f1", "f2", "f3"), "testEmptyFields",
      new Tuple("c1", "", "c3"), new Tuple("", "c2", "c3"), new Tuple("c1", "c2", ""));
  }

  @Test
  public void testMultilineContent()
  throws Exception {
    assertCheckScheme(new Fields("f1"), "testMultilineContent", new Tuple("line1 a\tb\nline2 c d"));
  }

  /**
   * Asserts that running the {@link CheckScheme} on the given file returns the expected {@link Tuple}s.
   * @param fields the {@link Fields} of the produced {@link Tuple}
   * @param test the name of the file to test
   * @param tuples the expected list of {@link Tuple}s
   * @throws IOException if an error occurs while reading the test file
   */
  private void assertCheckScheme(Fields fields, String test, Tuple...tuples)
  throws IOException {
    final String file = "./src/test/resources/testCheckScheme/" + test + ".check";
    final Tap source = new Hfs(new CheckScheme(fields), file);

    final JobConf conf = HadoopUtil.createJobConf(properties, new JobConf());
    final TupleEntryIterator it = source.openForRead(new HadoopFlowProcess(conf));
    try {
      for (Tuple tuple : tuples) {
        assertTrue(it.hasNext());
        assertEquals(tuple, it.next().getTuple());
      }

      assertFalse(it.hasNext());
    } finally {
      it.close();
    }
  }

}
