/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.junit.After;
import org.junit.Test;

import cascading.tuple.hadoop.TupleSerializationProps;

/**
 * 
 */
public class TestSerializationsLoader {

  @After
  public void tearDown() {
    SerializationsLoader.reset();
  }

  public static class TestSerialization implements Serialization<Void> {
    @Override
    public boolean accept(Class<?> c) {
      return false;
    }
    @Override
    public Serializer<Void> getSerializer(Class<Void> c) {
      return null;
    }
    @Override
    public Deserializer<Void> getDeserializer(Class<Void> c) {
      return null;
    }
  }

  @Test
  public void testLoadSerialization()
  throws Exception {
    final Properties p = new Properties();
    final String path = "src/test/resources/testSerializationsLoader/testLoadSerialization/serializations";

    SerializationsLoader.setSerializations(path);
    SerializationsLoader.load(p);
    assertEquals(TestSerialization.class.getName(), p.getProperty(TupleSerializationProps.HADOOP_IO_SERIALIZATIONS));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testNotSerialization()
  throws Exception {
    final Properties p = new Properties();
    final String path = "src/test/resources/testSerializationsLoader/testNotSerialization/notserializations";

    SerializationsLoader.setSerializations(path);
    SerializationsLoader.load(p);
  }

  @Test(expected=ClassNotFoundException.class)
  public void testNoClass()
  throws Exception {
    final Properties p = new Properties();
    final String path = "src/test/resources/testSerializationsLoader/testNoClass/noclass";

    SerializationsLoader.setSerializations(path);
    SerializationsLoader.load(p);
  }

}
