/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.rdf;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.junit.BeforeClass;
import org.junit.Test;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;

/**
 * 
 */
public class BenchRDFParser
extends AbstractBenchmark {

  private static final String file = "src/test/resources/bbc.nt.gz";
  private static final List<String> triples = new ArrayList<String>();
  private static final int nb = 50;

  @BeforeClass
  public static void load()
  throws Exception {
    final BufferedReader r = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
    String line = null;

    try {
      while ((line = r.readLine()) != null) {
        triples.add(line);
      }
    } finally {
      r.close();
    }
  }

  @Test
  public void testRDFParser1()
  throws Exception {
    for (int i = 0; i < nb; i++) {
      for (String nt : triples) {
        RDFParser1.parseStatement(nt);
      }
    }
  }

  @Test
  public void testRDFParser2()
  throws Exception {
    for (int i = 0; i < nb; i++) {
      for (String nt : triples) {
        RDFParser2.parseStatement(nt);
      }
    }
  }

  @Test
  public void testRDFParser3()
  throws Exception {
    for (int i = 0; i < nb; i++) {
      for (String nt : triples) {
        RDFParser3.parseStatement(nt);
      }
    }
  }

  @Test
  public void testRDFParser4()
  throws Exception {
    for (int i = 0; i < nb; i++) {
      for (String nt : triples) {
        RDFParser4.parseStatement(nt);
      }
    }
  }

}
