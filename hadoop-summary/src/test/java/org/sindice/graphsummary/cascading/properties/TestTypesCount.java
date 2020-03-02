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

/**
 * 
 */
public class TestTypesCount
extends AbstractSummaryTestCase {

  @Test
  public void testAdd1() {
    final TypesCount tc = new TypesCount();
    tc.add(1, (byte) 42);
    tc.add(1, (byte) 2);
    tc.add(1, (byte) 2);
    tc.add(2, (byte) 42);
    assertEquals(1l, tc.get(1l).get((byte) 42).longValue());
    assertEquals(2l, tc.get(1l).get((byte) 2).longValue());
    assertEquals(1l, tc.get(2l).get((byte) 42).longValue());
  }

  @Test
  public void testAdd2() {
    final TypesCount tc1 = new TypesCount();
    tc1.add(1, (byte) 42);
    tc1.add(2, (byte) 42);
    final TypesCount tc2 = new TypesCount();
    tc2.add(3, (byte) 42);
    tc2.add(2, (byte) 24);
    tc2.add(1, (byte) 42);

    tc1.add(tc2);
    assertEquals(2l, tc1.get(1l).get((byte) 42).longValue());
    assertEquals(1l, tc1.get(2l).get((byte) 42).longValue());
    assertEquals(1l, tc1.get(2l).get((byte) 24).longValue());
    assertEquals(1l, tc1.get(3l).get((byte) 42).longValue());
  }

  @Test
  public void testCompareTo()
  throws Exception {
    final TypesCount tc1 = new TypesCount();
    final TypesCount tc2 = new TypesCount();

    tc1.add(1l, (byte) 2);
    tc2.add(1l, (byte) 2);
    assertEquals(0, tc1.compareTo(tc1));
    assertEquals(0, tc1.compareTo(tc2));

    // tc1 < tc2
    tc2.clear();
    tc2.add(1l, (byte) 3);
    assertTrue(tc1.compareTo(tc2) < 0);
    assertTrue(tc2.compareTo(tc1) > 0);

    tc2.clear();
    tc2.add(1l, (byte) 2);
    tc2.add(1l, (byte) 2);
    assertTrue(tc1.compareTo(tc2) < 0);
    assertTrue(tc2.compareTo(tc1) > 0);

    // tc1 > tc2
    tc2.clear();
    tc2.add(1l, (byte) 1);
    assertTrue(tc1.compareTo(tc2) > 0);
    assertTrue(tc2.compareTo(tc1) < 0);

    tc1.add(1l, (byte) 2);
    tc2.clear();
    tc2.add(1l, (byte) 2);
    assertTrue(tc1.compareTo(tc2) > 0);
    assertTrue(tc2.compareTo(tc1) < 0);

    tc1.clear();
    tc1.add(1l, (byte) 2);
    tc2.clear();
    tc2.add(2l, (byte) 1);
    assertTrue(tc1.compareTo(tc2) < 0);
    assertTrue(tc2.compareTo(tc1) > 0);

    tc2.clear();
    tc2.add(0l, (byte) 1);
    assertTrue(tc1.compareTo(tc2) > 0);
    assertTrue(tc2.compareTo(tc1) < 0);

    tc2.clear();
    tc2.add(0l, (byte) 1);
    tc2.add(3l, (byte) 1);
    assertTrue(tc1.compareTo(tc2) > 0);
    assertTrue(tc2.compareTo(tc1) < 0);

    tc2.clear();
    tc2.add(1l, (byte) 2);
    tc2.add(3l, (byte) 1);
    assertTrue(tc1.compareTo(tc2) < 0);
    assertTrue(tc2.compareTo(tc1) > 0);
  }

  @Test
  public void testSerialization()
  throws Exception {
    final TypesCountSerialization serial = new TypesCountSerialization();
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final TypesCount pc = new TypesCount();

    pc.add(13l, (byte) 42);
    pc.add(42l, (byte) 13);
    pc.add(42l, (byte) 13);
    assertTrue(serial.accept(TypesCount.class));

    final Serializer<TypesCount> serializer = serial.getSerializer(TypesCount.class);
    serializer.open(out);
    serializer.serialize(pc);
    serializer.close();

    final Deserializer<TypesCount> deserializer = serial.getDeserializer(TypesCount.class);
    deserializer.open(new ByteArrayInputStream(out.toByteArray()));
    final TypesCount tc2 = new TypesCount();
    tc2.add(0l, (byte) 0);
    assertEquals(pc, deserializer.deserialize(tc2));
    deserializer.close();
  }

}
