/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.properties;


import java.io.ByteArrayInputStream;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serializer;
import org.junit.Test;
import org.sindice.graphsummary.AbstractSummaryTestCase;
import org.sindice.graphsummary.cascading.properties.PropertiesCount.Iterate;

/**
 * 
 */
public class TestPropertiesCount
extends AbstractSummaryTestCase {

  @Test
  public void testAdd1() {
    final PropertiesCount pc = new PropertiesCount();
    pc.add(1, 42);
    pc.add(2, 42);
    pc.add(1, 2);

    final Iterate it = pc.iterate();

    assertTrue(it.getNext());
    assertEquals(1l, it.getProperty());
    assertEquals(2l, it.getDatatype());
    assertEquals(1l, it.getCount());

    assertTrue(it.getNext());
    assertEquals(1l, it.getProperty());
    assertEquals(42l, it.getDatatype());
    assertEquals(1l, it.getCount());

    assertTrue(it.getNext());
    assertEquals(2l, it.getProperty());
    assertEquals(42l, it.getDatatype());
    assertEquals(1l, it.getCount());

    assertFalse(it.getNext());
  }

  @Test
  public void testAdd2() {
    final PropertiesCount pc1 = new PropertiesCount();
    pc1.add(1, 42);
    pc1.add(2, 42);
    final PropertiesCount pc2 = new PropertiesCount();
    pc2.add(3, 42);
    pc2.add(2, 24);
    pc2.add(1, 42);

    pc1.add(pc2);

    final Iterate it = pc1.iterate();

    assertTrue(it.getNext());
    assertEquals(1l, it.getProperty());
    assertEquals(42l, it.getDatatype());
    assertEquals(2l, it.getCount());

    assertTrue(it.getNext());
    assertEquals(2l, it.getProperty());
    assertEquals(24l, it.getDatatype());
    assertEquals(1l, it.getCount());

    assertTrue(it.getNext());
    assertEquals(2l, it.getProperty());
    assertEquals(42l, it.getDatatype());
    assertEquals(1l, it.getCount());

    assertTrue(it.getNext());
    assertEquals(3l, it.getProperty());
    assertEquals(42l, it.getDatatype());
    assertEquals(1l, it.getCount());

    assertFalse(it.getNext());
  }

  @Test
  public void testAdd3() {
    final PropertiesCount pc = new PropertiesCount();
    pc.add(1, 10);
    pc.add(1, 30);
    pc.add(1, 20);

    final Iterate it = pc.iterate();

    assertTrue(it.getNext());
    assertEquals(1l, it.getProperty());
    assertEquals(10l, it.getDatatype());
    assertEquals(1l, it.getCount());

    assertTrue(it.getNext());
    assertEquals(1l, it.getProperty());
    assertEquals(20l, it.getDatatype());
    assertEquals(1l, it.getCount());

    assertTrue(it.getNext());
    assertEquals(1l, it.getProperty());
    assertEquals(30l, it.getDatatype());
    assertEquals(1l, it.getCount());

    assertFalse(it.getNext());
  }

  @Test
  public void testCompareTo()
  throws Exception {
    final PropertiesCount pc1 = new PropertiesCount();
    final PropertiesCount pc2 = new PropertiesCount();

    pc1.add(1l, 42l, 2l);
    pc2.add(1l, 42l, 2l);
    assertEquals(0, pc1.compareTo(pc1));
    assertEquals(0, pc1.compareTo(pc2));

    // pc1 < pc2
    pc2.clear();
    pc2.add(1l, 42l, 3l);
    assertTrue(pc1.compareTo(pc2) < 0);
    assertTrue(pc2.compareTo(pc1) > 0);

    // pc1 > pc2
    pc2.clear();
    pc2.add(1l, 42l, 1l);
    assertTrue(pc1.compareTo(pc2) > 0);
    assertTrue(pc2.compareTo(pc1) < 0);

    // pc1 < pc2
    pc2.clear();
    pc2.add(2l, 42l, 1l);
    assertTrue(pc1.compareTo(pc2) < 0);
    assertTrue(pc2.compareTo(pc1) > 0);

    // pc1 > pc2
    pc2.clear();
    pc2.add(0l, 42l, 1l);
    assertTrue(pc1.compareTo(pc2) > 0);
    assertTrue(pc2.compareTo(pc1) < 0);

    // pc1 > pc2
    pc2.clear();
    pc2.add(0l, 42l, 1l);
    pc2.add(3l, 42l, 1l);
    assertTrue(pc1.compareTo(pc2) > 0);
    assertTrue(pc2.compareTo(pc1) < 0);

    // pc1 < pc2
    pc2.clear();
    pc2.add(1l, 42l, 2l);
    pc2.add(3l, 42l, 1l);
    assertTrue(pc1.compareTo(pc2) < 0);
    assertTrue(pc2.compareTo(pc1) > 0);

    // with datatypes

    // pc1 > pc2
    pc2.clear();
    pc2.add(1l, 24l, 2l);
    assertTrue(pc1.compareTo(pc2) > 0);
    assertTrue(pc2.compareTo(pc1) < 0);

    // pc1 < pc2
    pc2.clear();
    pc2.add(1l, 43l, 2l);
    assertTrue(pc1.compareTo(pc2) < 0);
    assertTrue(pc2.compareTo(pc1) > 0);
  }

  @Test
  public void testSerialization1()
  throws Exception {
    final PropertiesCountSerialization serial = new PropertiesCountSerialization();
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final PropertiesCount pc = new PropertiesCount();

    pc.add(13l, 24l, 42l);
    pc.add(42l, 35l, 13l);
    pc.add(1l, 24l, 1l);
    assertTrue(serial.accept(PropertiesCount.class));

    final Serializer<PropertiesCount> serializer = serial.getSerializer(PropertiesCount.class);
    serializer.open(out);
    serializer.serialize(pc);
    serializer.close();

    final Deserializer<PropertiesCount> deserializer = serial.getDeserializer(PropertiesCount.class);
    deserializer.open(new ByteArrayInputStream(out.toByteArray()));
    final PropertiesCount pc2 = new PropertiesCount();
    pc2.add(0l, 0l);
    assertEquals(pc, deserializer.deserialize(pc2));
    deserializer.close();
  }

  @Test
  public void testSerialization2()
  throws Exception {
    final PropertiesCountSerialization serial = new PropertiesCountSerialization();
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final PropertiesCount pc = new PropertiesCount();

    pc.add(13l, 24l, 42l);
    assertTrue(serial.accept(PropertiesCount.class));

    final Serializer<PropertiesCount> serializer = serial.getSerializer(PropertiesCount.class);
    serializer.open(out);
    serializer.serialize(pc);
    serializer.close();

    final Deserializer<PropertiesCount> deserializer = serial.getDeserializer(PropertiesCount.class);
    deserializer.open(new ByteArrayInputStream(out.toByteArray()));
    final PropertiesCount pc2 = deserializer.deserialize(null);

    pc.add(13l, 24l, 42l);
    pc2.add(13l, 24l, 42l);
    assertEquals(pc, pc2);
    deserializer.close();
  }

}
