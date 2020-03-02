/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.entity;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFParser.DatatypeHandling;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.rio.ntriples.NTriplesParser;
import org.openrdf.rio.ntriples.NTriplesUtil;
import org.sindice.analytics.entity.EntityDescription.Statements;
import org.sindice.core.analytics.util.Hash;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;

/**
 * 
 */
@RunWith(value = Parameterized.class)
public class TestEntityDescription {

  private final EntityDescription eDesc;
  private final FlowProcess       fp;

  private final String            entity = "http://s.com";

  public TestEntityDescription(EntityDescription eDesc, final FlowProcess fp) {
    this.eDesc = eDesc;
    this.fp = fp;
  }

  @Before
  public void setUp() {
    eDesc.reset();
  }

  @Parameters
  public static Collection<Object[]> data() {
    final FlowProcess fp = new HadoopFlowProcess();

    Object[][] data = new Object[][] {
      { new MapEntityDescription(), fp }
    };
    return Arrays.asList(data);
  }

  @Test
  public void testEmptyLiteral()
  throws Exception {
    final String[] e = {
      "<http://s.com> <http://p1.com> \"\" .\n",
      "<http://s.com> <http://p1.com> \"bla\" .\n",
      "<http://s.com> <http://p2.com> \"bla\" .\n",
      "<http://s.com> <http://p2.com> \"\" .\n"
    };
    this.assertBuild(eDesc, e);

    // iterate through them
    this.assertStatements(entity, eDesc, e);
  }

  @Test
  public void testLiteralWithDatatype()
  throws Exception {
    final String e = "<http://s.com> <http://p1.com> \"spock\"^^<http://vulcan.org> .\n";

    this.assertBuild(eDesc, e);

    this.assertStatements(entity, eDesc, e);
  }

  @Test
  public void testDuplicate()
  throws Exception {
    final String e = "<http://s.com> <http://p1.com> <http://o1.com> .\n";

    this.assertBuild(eDesc, e);

    // iterate through them
    this.assertStatements(entity, eDesc, e);
  }

  @Test
  public void testGetEntity()
  throws Exception {
    final String nt = "<http://lod.b3kat.de/isbn/3921846366\\u003E980\\u003E> " +
                      "<http://www.w3.org/2002/07/owl#sameAs> " +
                      "<http://lod.b3kat.de/title/BV020820449> .";

    this.assertBuild(eDesc, nt);
    assertStatements("http://lod.b3kat.de/isbn/3921846366>980>",
      eDesc, "<http://lod.b3kat.de/isbn/3921846366>980>> <http://www.w3.org/2002/07/owl#sameAs> " +
      "<http://lod.b3kat.de/title/BV020820449> .\n");
  }

  @Test
  public void testAdd1()
  throws Exception {
    final String[] e = {
      "<http://s.com> <http://p1.com> <http://o1.com> .\n",
      "<http://s.com> <http://p2.com> <http://o3.com> .\n",
      "<http://s.com> <http://p1.com> <http://o2.com> .\n"
    };

    this.assertBuild(eDesc, e);

    // iterate through them
    this.assertStatements(entity, eDesc, e);
  }

  @Test
  public void testAdd2()
  throws Exception {
    final String[] e = {
      "<http://s.com> <http://p1.com> <http://o1.com> .\n",
      "<http://s.com> <http://p2.com> <http://o3.com> .\n",
      "<http://s.com> <http://p1.com> <http://o2.com> .\n"
    };

    this.assertBuild(eDesc, e);

    // iterate through them
    this.assertStatements(entity, eDesc, e);
  }

  @Test
  public void testAdd3()
  throws Exception {
    final String[] e = {
      "<http://s.com> <http://p1.com> \"o1\" .\n",
      "<http://s.com> <http://p2.com> <http://o1.com> .\n",
      "<http://s.com> <http://p1.com> <http://o2.com> .\n"
    };

    this.assertBuild(eDesc, e);

    // iterate through them
    this.assertStatements(entity, eDesc, e);
  }

  @Test
  public void testAddEntity1()
  throws Exception {
    final String[] triples = {
      "<http://s.com> <http://p1.com> \"o1\" .\n",
      "<http://s.com> <http://p2.com> \"o2\" .\n"
    };
    final EntityDescription e2 = EntityDescriptionFactory.getEntityDescription(fp);

    assertBuild(eDesc, triples[0]);
    assertBuild(e2, triples[1]);

    eDesc.add(e2);
    assertStatements("http://s.com", eDesc, triples);
  }

  @Test
  public void testAddEntity2()
  throws Exception {
    final String[] triples = {
      "<http://s.com> <http://p1.com> \"o1\" .\n",
      "<http://s.com> <http://p1.com> \"o2\" .\n"
    };
    final EntityDescription e2 = EntityDescriptionFactory.getEntityDescription(fp);

    assertBuild(eDesc, triples[0]);
    assertBuild(e2, triples[1]);

    eDesc.add(e2);
    assertStatements("http://s.com", eDesc, triples);
  }

  @Test
  public void testEmpty()
  throws Exception {
    this.assertBuild(eDesc, new String[] {});
    assertStatements(null, eDesc, new String[] {});
  }

  @Test
  public void testEqualsHashcode1()
  throws Exception {
    final String triple = "_:b1 <http://foaf.com/knows> \"frodo\" .";
    final EntityDescription e2 = EntityDescriptionFactory.getEntityDescription(fp);

    assertBuild(eDesc, triple);
    assertBuild(e2, triple);

    assertEquals(eDesc, e2);
    assertEquals(eDesc.hashCode(), e2.hashCode());
  }

  @Test
  public void testEqualsHashcode2()
  throws Exception {
    final String triple1 = "_:b1 <http://foaf.com/knows> \"frodo\" .";
    final String triple2 = "_:b2 <http://foaf.com/knows> \"frodo\" .";
    final EntityDescription e2 = EntityDescriptionFactory.getEntityDescription(fp);

    assertBuild(eDesc, triple1);
    assertBuild(e2, triple2);

    // different because different entities
    assertFalse(eDesc.equals(e2));
    assertFalse(eDesc.hashCode() == e2.hashCode());
  }

  @Test
  public void testEqualsHashcode3()
  throws Exception {
    final EntityDescription e2 = EntityDescriptionFactory.getEntityDescription(fp);

    assertEquals(eDesc, e2);
    assertEquals(eDesc.hashCode(), e2.hashCode());
  }

  @Test
  public void testPrefixes()
  throws Exception {
    final String[] triples = {
      "_:b1 <http://foaf.com/knows> \"frodo\"^^<http://middle-earth#hobbit> .\n",
      "_:b1 <http://foaf.com/kills> \"frodo\"^^<http://middle-earth#sauron> .\n"
    };

    assertBuild(eDesc, triples);
    assertStatements("b1", eDesc, triples);
  }

  @Test
  public void testPrefixNoLocalname()
  throws Exception {
    final String[] triples = {
      "_:b1 <http://middle-earth#> \"\"^^<http://middle-earth#> .\n"
    };
  
    assertBuild(eDesc, triples);
    assertStatements("b1", eDesc, triples);
  }

  /**
   * Adds the triples to the given entity
   * @param eDesc the {@link EntityDescription} to test
   * @param triples the statements to be added
   * @throws Exception
   */
  private void assertBuild(EntityDescription eDesc, String...triples)
  throws Exception {
    final NTriplesParser parser = new NTriplesParser();
    final StatementCollector handler = new StatementCollector();

    parser.setRDFHandler(handler);
    parser.setPreserveBNodeIDs(true);
    parser.setDatatypeHandling(DatatypeHandling.IGNORE);
    parser.setVerifyData(false);

    // add the triples to the entity
    for (String nt : triples) {
      handler.clear();
      parser.parse(new StringReader(nt), "");
      if (handler.getStatements().isEmpty()) {
        continue;
      }
      final Statement st = handler.getStatements().iterator().next();
      eDesc.add(AnalyticsValue.fromSesame(st.getSubject()), AnalyticsValue.fromSesame(st.getPredicate()),
        AnalyticsValue.fromSesame(st.getObject()));
    }
    assertEquals(triples.length, eDesc.getNbStatements());
  }

  /**
   * Iterates over and asserts the statements of this entity.
   * @param entity the entity identifier
   * @param eDesc the {@link EntityDescription} to assert
   * @param triples the expected statements
   */
  private void assertStatements(String entity, EntityDescription eDesc, String...triples)
  throws IOException {
    final ByteArrayOutputStream bout = new ByteArrayOutputStream();

    // before serialization
    doAssertStatements(eDesc, entity, triples);

    // serialize + deserialize
    final EntityDescriptionSerialization serial = new EntityDescriptionSerialization();
    assertTrue(serial.accept(eDesc.getClass()));
    final Serializer<EntityDescription> s = serial.getSerializer(eDesc.getClass());
    s.open(bout);
    s.serialize(eDesc);
    s.close();
    final Deserializer<EntityDescription> d = serial.getDeserializer(eDesc.getClass());
    d.open(new ByteArrayInputStream(bout.toByteArray()));

    // after serialization
    doAssertStatements(d.deserialize(null), entity, triples);

    d.close();
  }

  private void doAssertStatements(EntityDescription eDesc, String entity, String...triples) {
    if (entity == null) {
      assertEquals(null, eDesc.getEntity());
    } else {
      assertEquals(entity, eDesc.getEntity().stringValue());
    }
    assertEquals(triples.length, eDesc.getNbStatements());

    final Statements st = eDesc.iterateStatements();
    final List<String> nts = new ArrayList<String>();
    final Value subject = eDesc.getEntity();
    final StringBuilder sb = new StringBuilder();

    while (st.getNext()) {
      sb.setLength(0);
      sb.append(NTriplesUtil.toNTriplesString(subject)).append(' ')
        .append(NTriplesUtil.toNTriplesString(st.getPredicate())).append(' ')
        .append(NTriplesUtil.toNTriplesString(st.getObject())).append(" .\n");

      nts.add(sb.toString());
      // assert the hash values
      final AnalyticsValue p = st.getPredicate();
      final AnalyticsValue o = st.getObject();
      assertEquals(Hash.getHash64(p.getValue()), st.getPredicateHash64());
      assertEquals(Hash.getHash64(o.getValue()), st.getObjectHash64());
      assertEquals(BlankNode.getHash128(o), st.getObjectHash128());
    }
    Collections.sort(nts);
    Arrays.sort(triples);
    assertArrayEquals(triples, nts.toArray(new String[0]));
  }

}
