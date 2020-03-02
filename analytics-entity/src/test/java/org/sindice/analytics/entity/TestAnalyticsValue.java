/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.entity;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.junit.Test;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;

/**
 * 
 */
public class TestAnalyticsValue {

  @Test
  public void testGetLocalNameIndex()
  throws Exception {
    final String uri1 = "dog:gri/play#lo#ts";
    final String uri2 = "dog:gri/play/lots";
    final String uri3 = "dog:dog:gri";
    final byte[] uri1Bytes = uri1.getBytes(Charset.forName("UTF-8"));
    final byte[] uri2Bytes = uri2.getBytes(Charset.forName("UTF-8"));
    final byte[] uri3Bytes = uri3.getBytes(Charset.forName("UTF-8"));

    assertEquals(uri1.indexOf('#') + 1, AnalyticsUri.getLocalNameIndex(uri1Bytes, 0, uri1Bytes.length));
    assertEquals(uri2.lastIndexOf('/') + 1, AnalyticsUri.getLocalNameIndex(uri2Bytes, 0, uri2Bytes.length));
    assertEquals(uri3.lastIndexOf(':') + 1, AnalyticsUri.getLocalNameIndex(uri3Bytes, 0, uri3Bytes.length));
  }

  @Test
  public void testNamespaceLocalName()
  throws Exception {
    final AnalyticsUri uri = new AnalyticsUri("http://acme.org/me#test");
    final URI sesUri = new URIImpl("http://acme.org/me#test");

    assertEquals(sesUri.getNamespace(), uri.getNamespace());
    assertEquals(sesUri.getLocalName(), uri.getLocalName());
  }

  @Test
  public void testURI()
  throws IOException {
    assertAnalyticsValueSerialization(new AnalyticsUri("http://grishka.org"));
  }

  @Test
  public void testBNode()
  throws IOException {
    assertAnalyticsValueSerialization(new AnalyticsBNode("bnode1"));
  }

  @Test
  public void testLiteral()
  throws IOException {
    assertAnalyticsValueSerialization(new AnalyticsLiteral("toto"), new AnalyticsLiteral("toto", "datatype", null),
      new AnalyticsLiteral("toto", null, "lang"), new AnalyticsLiteral(""), new AnalyticsLiteral("", "datatype", null),
      new AnalyticsLiteral("", null, "lang"));
  }

  @Test
  public void testMix()
  throws IOException {
    assertAnalyticsValueSerialization(new AnalyticsUri("http://grishka.org"), new AnalyticsBNode("bnode1"),
      new AnalyticsLiteral("toto"));
  }

  /**
   * Asserts that the list of {@link AnalyticsValue}s are correctly de/serialized
   * using {@link AnalyticsValueSerialization}.
   */
  private void assertAnalyticsValueSerialization(AnalyticsValue... values)
  throws IOException {
    final WritableSerialization serial = new WritableSerialization();
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    final Serializer<Writable> serializer = serial.getSerializer(Writable.class);
    serializer.open(out);
    for (AnalyticsValue value : values) {
      assertTrue(serial.accept(value.getClass()));
      serializer.serialize(value);
    }
    serializer.close();

    final InputStream in = new ByteArrayInputStream(out.toByteArray());
    for (AnalyticsValue value : values) {
      Object v = value.getClass();
      final Deserializer<Writable> deserializer = serial.getDeserializer((Class<Writable>) v);
      deserializer.open(in);
      assertEquals(value, deserializer.deserialize(null));
      deserializer.close();
    }
  }

  /**
   * See {@link AnalyticsValue#getUniqueClassID()}
   */
  @Test
  public void testCompareTo()
  throws Exception {
    final List<AnalyticsValue> values = new ArrayList<AnalyticsValue>();

    values.add(new AnalyticsBNode("value"));
    values.add(new AnalyticsUri("value"));
    values.add(new AnalyticsBNode("value"));
    values.add(new AnalyticsLiteral("value"));
    values.add(new AnalyticsLiteral("value"));
    values.add(new AnalyticsUri("value"));
    Collections.sort(values);

    assertEquals(new AnalyticsUri("value"), values.get(0));
    assertEquals(new AnalyticsUri("value"), values.get(1));
    assertEquals(new AnalyticsBNode("value"), values.get(2));
    assertEquals(new AnalyticsBNode("value"), values.get(3));
    assertEquals(new AnalyticsLiteral("value"), values.get(4));
    assertEquals(new AnalyticsLiteral("value"), values.get(5));
  }

  @Test
  public void testCopy1() {
    final AnalyticsLiteral lit = new AnalyticsLiteral("lit");
    final AnalyticsBNode bnode = new AnalyticsBNode("bnode");
    final AnalyticsUri uri = new AnalyticsUri("uri");

    assertEquals(lit, lit.getCopy());
    assertEquals(bnode, bnode.getCopy());
    assertEquals(uri, uri.getCopy());
  }

  /**
   * GL-124
   */
  @Test
  public void testCopy2() {
    final AnalyticsLiteral lit = new AnalyticsLiteral("lit", "datatype", null);
    final Literal sesLit = new LiteralImpl("toto", "fr");

    final AnalyticsValue v = AnalyticsValue.fromSesame(sesLit, new AnalyticsValue[] { new AnalyticsUri(),
      new AnalyticsBNode(), lit });
    assertEquals(v, v.getCopy());
  }

  @Test
  public void testEqualsHashcode1()
  throws Exception {
    final AnalyticsUri uri1 = new AnalyticsUri("http://grishka.org");
    final AnalyticsUri uri2 = new AnalyticsUri("http://grishka.org");
    final AnalyticsUri uri3 = new AnalyticsUri("http://dog.org");

    assertEqualsHashcode(uri1, uri2, uri3);
  }

  @Test
  public void testEqualsHashcode2()
  throws Exception {
    final AnalyticsLiteral lit1 = new AnalyticsLiteral("grishka");
    final AnalyticsLiteral lit2 = new AnalyticsLiteral("grishka");
    final AnalyticsLiteral lit3 = new AnalyticsLiteral("cat");

    assertEqualsHashcode(lit1, lit2, lit3);

    final AnalyticsLiteral lit1Dt = new AnalyticsLiteral("grishka", "datatype:dog", null);
    final AnalyticsLiteral lit2Dt = new AnalyticsLiteral("grishka", "datatype:dog", null);
    final AnalyticsLiteral lit3Dta = new AnalyticsLiteral("cat", "datatype:dog", null);
    final AnalyticsLiteral lit3Dtb = new AnalyticsLiteral("grishka", "datatype:cat", null);

    assertEqualsHashcode(lit1Dt, lit2Dt, lit3Dta);
    assertEqualsHashcode(lit1Dt, lit2Dt, lit3Dtb);

    final AnalyticsLiteral lit1Lang = new AnalyticsLiteral("grishka", null, "vulcan");
    final AnalyticsLiteral lit2Lang = new AnalyticsLiteral("grishka", null, "vulcan");
    final AnalyticsLiteral lit3Langa = new AnalyticsLiteral("cat", null, "vulcan");
    final AnalyticsLiteral lit3Langb = new AnalyticsLiteral("grishka", null, "bark");

    assertEqualsHashcode(lit1Lang, lit2Lang, lit3Langa);
    assertEqualsHashcode(lit1Lang, lit2Lang, lit3Langb);
  }

  @Test
  public void testEqualsHashcode3()
  throws Exception {
    final AnalyticsBNode bnode1 = new AnalyticsBNode("bnode1");
    final AnalyticsBNode bnode2 = new AnalyticsBNode("bnode1");
    final AnalyticsBNode bnode3 = new AnalyticsBNode("bnode3");

    assertEqualsHashcode(bnode1, bnode2, bnode3);
  }

  @Test
  public void testEqualsHashcode4()
  throws Exception {
    final AnalyticsBNode v1 = new AnalyticsBNode("grishka");
    final AnalyticsUri v2 = new AnalyticsUri("grishka");
    final AnalyticsLiteral v3 = new AnalyticsLiteral("grishka");

    assertFalse(v1.equals(v2));
    assertFalse(v2.equals(v1));
    assertFalse(v1.equals(v3));
    assertFalse(v3.equals(v1));

    assertFalse(v1.hashCode() == v2.hashCode());
    assertFalse(v1.hashCode() == v3.hashCode());
  }

  @Test
  public void testBytesRef()
  throws Exception {
    final byte[] b1 = { 42, 0, 1, 2, 43 };
    final byte[] b2 = { 43, 44, 0, 1, 2 };

    final AnalyticsUri uri1 = new AnalyticsUri(b1, 1, 3);
    final AnalyticsUri uri2 = new AnalyticsUri(b2, 2, 3);
    assertEquals(uri1, uri2);
    assertEquals(uri1.hashCode(), uri2.hashCode());
    assertEquals(0, uri1.compareTo(uri2));
  }

  @Test
  public void testAsBytesWritable()
  throws Exception {
    final AnalyticsUri uri = new AnalyticsUri("grishka");
    final BytesWritable uriBw = uri.getValueAsBytesWritable();
    assertEquals(uri, new AnalyticsUri(uriBw.getBytes(), 0, uriBw.getLength()));

    final AnalyticsBNode bnode = new AnalyticsBNode("grishka");
    final BytesWritable bnodeBw = bnode.getValueAsBytesWritable();
    assertEquals(bnode, new AnalyticsBNode(bnodeBw.getBytes(), 0, bnodeBw.getLength()));

    final AnalyticsLiteral lit = new AnalyticsLiteral("grishka");
    final BytesWritable litBw = lit.getValueAsBytesWritable();
    assertEquals(lit, new AnalyticsLiteral(litBw.getBytes(), 0, litBw.getLength()));
  }

  /**
   * Asserts that
   * <ul>
   * <li>v1 is equal to itself</li>
   * <li>v1 and v2 are equals</li>
   * <li>v1 and v3 are not equal</li>
   * <li>v1 and v2 have the same hashcode</li>
   * <li>v1 and v3 do not have the same hashcode</li>
   * </ul>
   * @param v1 of type {@link AnalyticsValue}
   * @param v2 of type {@link AnalyticsValue}
   * @param v3 of type {@link AnalyticsValue}
   */
  private void assertEqualsHashcode(final AnalyticsValue v1, final AnalyticsValue v2, final AnalyticsValue v3) {
    // equals
    assertEquals(v1, v1);
    assertEquals(v1, v2);
    assertFalse(v3.equals(v1));
    // hashcode
    assertEquals(v1.hashCode(), v2.hashCode());
    assertFalse(v1.hashCode() == v3.hashCode());
  }

}
