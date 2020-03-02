/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.rdf;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.sindice.core.analytics.rdf.AnalyticsClassAttributes;


/**
 * 
 */
public class TestAnalyticsClassAttributes {

  public static final String PRED_1  = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
  public static final String PRED_2  = "http://opengraph.org/schema/type";
  public static final String PRED_3  = "http://opengraphprotocol.org/schema/type";
  public static final String PRED_4  = "http://ogp.me/ns#type";
  public static final String PRED_5  = "http://purl.org/dc/elements/1.1/type";
  public static final String PRED_6  = "http://dbpedia.org/property/type";
  public static final String PRED_7  = "http://dbpedia.org/ontology/type";

  public static final String PRED_1B = "http://www.w3.org/1999/02/22-rdf-syntax-ns#TYPE";
  public static final String PRED_2B = "opengraph.org/schema/type";
  public static final String PRED_3B = "http://www.sindice.com/type";

  @Before
  public void setUp()
  throws Exception {
    AnalyticsClassAttributes.initClassAttributes(new String[] {
      "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
      "http://opengraphprotocol.org/schema/type",
      "http://opengraph.org/schema/type",
      "http://ogp.me/ns#type",
      "http://purl.org/dc/elements/1.1/type",
      "http://purl.org/stuff/rev#type", //Added 19 Oct 2011
      "http://purl.org/dc/terms/type", //Added 19 Oct 2011
      "http://dbpedia.org/property/type",
      "http://dbpedia.org/ontology/type",
      "http://dbpedia.org/ontology/Organisation/type", //Added 25 Oct 2011
      "http://xmlns.com/foaf/0.1/type", //Added 25 Oct 2011
    });
  }

  @Test
  /**
   * Verifies Analytics commons recognises correct predicates to define a class
   */
  public void testClassDefinitions() {

    assertEquals(true, AnalyticsClassAttributes.isClass(PRED_1));
    assertEquals(true, AnalyticsClassAttributes.isClass(PRED_2));
    assertEquals(true, AnalyticsClassAttributes.isClass(PRED_3));
    assertEquals(true, AnalyticsClassAttributes.isClass(PRED_4));
    assertEquals(true, AnalyticsClassAttributes.isClass(PRED_5));
    assertEquals(true, AnalyticsClassAttributes.isClass(PRED_6));
    assertEquals(true, AnalyticsClassAttributes.isClass(PRED_7));

    assertEquals(false, AnalyticsClassAttributes.isClass(PRED_1B));
    assertEquals(false, AnalyticsClassAttributes.isClass(PRED_2B));
    assertEquals(false, AnalyticsClassAttributes.isClass(PRED_3B));
  }

  @Test
  public void testLiteralType()
  throws Exception {
    assertEquals("\"movie\"", AnalyticsClassAttributes.normalizeLiteralType("\"   MOVIE \""));
    assertEquals("\"class as sentence\"", AnalyticsClassAttributes.normalizeLiteralType("\"   class AS  SEntence   \""));
    assertEquals("movie", AnalyticsClassAttributes.normalizeLiteralTypeLabel("   MOVIE "));
    assertEquals("class as sentence", AnalyticsClassAttributes.normalizeLiteralTypeLabel("   class AS  SEntence   "));

    assertEquals("\"test test\"", AnalyticsClassAttributes.normalizeLiteralType("\"    test  test  \""));
    assertEquals("\"test test\"", AnalyticsClassAttributes.normalizeLiteralType("\" test \n test  \""));
    assertEquals("\"test test\"", AnalyticsClassAttributes.normalizeLiteralType("\" test \t\n TEST  \""));
    assertEquals("test test", AnalyticsClassAttributes.normalizeLiteralTypeLabel("    test  test  "));
    assertEquals("test test", AnalyticsClassAttributes.normalizeLiteralTypeLabel("    test \n test  "));
    assertEquals("test test", AnalyticsClassAttributes.normalizeLiteralTypeLabel("\t  test \t\n TEST \n \n"));
    assertEquals("en", AnalyticsClassAttributes.normalizeLiteralTypeLabel("en"));
  }

}
