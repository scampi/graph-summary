/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.properties;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serializer;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sindice.core.analytics.util.Hash;
import org.sindice.core.analytics.util.ReusableByteArrayOutputStream;
import org.sindice.graphsummary.cascading.properties.PropertiesCount;
import org.sindice.graphsummary.cascading.properties.PropertiesCountSerialization;

import cascading.tuple.hadoop.io.BufferedInputStream;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;

/**
 * 
 */
public class BenchPropertiesCount
extends AbstractBenchmark {

  private static final String     file       = "src/test/resources/isbn.gz";
  private static final List<Long> predicates = new ArrayList<Long>();
  private static final int        nb         = 50;

  private final PropertiesCount1  p1         = new PropertiesCount1();
  private final PropertiesCount   p2         = new PropertiesCount();

  @BeforeClass
  public static void load()
  throws Exception {
    final BufferedReader r = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
    String line = null;

    try {
      while ((line = r.readLine()) != null) {
        predicates.add(Hash.getHash64(line));
      }
    } finally {
      r.close();
    }
  }

  @Test
  public void testAdd1()
  throws Exception {
    for (int i = 0; i < nb; i++) {
      p1.clear();
      for (long label : predicates) {
        p1.add(label, 0);
      }
    }
  }

  @Test
  public void testAdd2()
  throws Exception {
    for (int i = 0; i < nb; i++) {
      p2.clear();
      for (long label : predicates) {
        p2.add(label, 0);
      }
    }
  }

  @Test
  public void testSerialization1()
  throws Exception {
    p1.clear();
    for (long label : predicates) {
      p1.add(label, 0);
    }

    final PropertiesCountSerialization1 s = new PropertiesCountSerialization1();
    final Serializer<PropertiesCount1> ser = s.getSerializer(PropertiesCount1.class);
    final Deserializer<PropertiesCount1> deser = s.getDeserializer(PropertiesCount1.class);
    final ReusableByteArrayOutputStream bytes = new ReusableByteArrayOutputStream(1024);
    final BufferedInputStream in = new BufferedInputStream();

    ser.open(bytes);
    deser.open(in);
    for (int i = 0; i < nb; i++) {
      bytes.reset();
      ser.serialize(p1);
      in.reset(bytes.getBytes(), 0, bytes.size());
      deser.deserialize(null);
    }
  }

  @Test
  public void testSerialization2()
  throws Exception {
    p2.clear();
    for (long label : predicates) {
      p2.add(label, 0);
    }

    final PropertiesCountSerialization s = new PropertiesCountSerialization();
    final Serializer<PropertiesCount> ser = s.getSerializer(PropertiesCount.class);
    final Deserializer<PropertiesCount> deser = s.getDeserializer(PropertiesCount.class);
    final ReusableByteArrayOutputStream bytes = new ReusableByteArrayOutputStream(1024);
    final BufferedInputStream in = new BufferedInputStream();

    ser.open(bytes);
    deser.open(in);
    for (int i = 0; i < nb; i++) {
      bytes.reset();
      ser.serialize(p2);
      in.reset(bytes.getBytes(), 0, bytes.size());
      deser.deserialize(null);
    }
  }

}
