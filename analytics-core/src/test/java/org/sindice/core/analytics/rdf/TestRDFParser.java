/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.rdf;

import java.util.List;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
import org.sindice.core.analytics.testHelper.AbstractAnalyticsTestCase;
import org.sindice.core.analytics.util.AnalyticsException;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class TestRDFParser
extends AbstractAnalyticsTestCase {

  @Test
  public void testNTriplesWithoutDefaultDomain()
  throws Exception {
    final String nt = "<http://acme.org/a> <http://acme.org/name> \"roger\" .";
    final TupleEntry te = new TupleEntry(new Tuple(nt));

    final JobConf conf = new JobConf();
    conf.set(AnalyticsParameters.DOCUMENT_FORMAT.toString(), DocumentFormat.NTRIPLES.toString());

    final RDFParser parser = new RDFParser(new HadoopFlowProcess(conf));
    final RDFDocument rdf = parser.getRDFDocument(te);

    assertEquals("sindice.com", rdf.getDatasetLabel());
    assertEquals("http://sindice.com", rdf.getUrl());
    final List<Triple> triples = rdf.getTriples();
    assertEquals(1, triples.size());
    assertEquals("http://acme.org/a", triples.get(0).getSubjectString());
    assertEquals("http://acme.org/name", triples.get(0).getPredicateString());
    assertEquals("roger", triples.get(0).getObjectString());
  }

  @Test
  public void testNQuads()
  throws Exception {
    final String nq = "<http://worldbank.270a.info/classification/project/P126357> "
                      + "<http://worldbank.270a.info/property/references> "
                      + "_:P126357references43 <http://worldbank.270a.info/> .";
    final TupleEntry te = new TupleEntry(new Tuple(nq));
    final JobConf conf = new JobConf();
    conf.set(AnalyticsParameters.DOCUMENT_FORMAT.toString(), DocumentFormat.NQUADS.toString());
    final RDFParser parser = new RDFParser(new HadoopFlowProcess(conf));
    final RDFDocument rdf = parser.getRDFDocument(te);

    assertEquals("270a.info", rdf.getDatasetLabel());
    assertEquals("http://worldbank.270a.info/", rdf.getUrl());
    final List<Triple> triples = rdf.getTriples();
    assertEquals(1, triples.size());
    assertEquals("http://worldbank.270a.info/classification/project/P126357",
      triples.get(0).getSubjectString());
    assertEquals("http://worldbank.270a.info/property/references",
      triples.get(0).getPredicateString());
    assertEquals("P126357references43",
      triples.get(0).getObjectString());
  }

  @Test
  public void testNQuadsList()
  throws Exception {
    final String nq = "<http://acme.org/s1> <http://acme.org/p1> <http://acme.org/o1> <http://acme.org/> .\n" +
                      "<http://acme.org/s2> <http://acme.org/p2> <http://acme.org/o2> <http://acme.org/> .";
    final TupleEntry te = new TupleEntry(new Tuple(nq));
    final JobConf conf = new JobConf();
    conf.set(AnalyticsParameters.DOCUMENT_FORMAT.toString(), DocumentFormat.NQUADS.toString());
    final RDFParser parser = new RDFParser(new HadoopFlowProcess(conf));
    final RDFDocument rdf = parser.getRDFDocument(te);

    assertEquals("acme.org", rdf.getDatasetLabel());
    assertEquals("http://acme.org/", rdf.getUrl());

    final List<Triple> triples = rdf.getTriples();
    assertEquals(2, triples.size());

    assertEquals("http://acme.org/s1", triples.get(0).getSubjectString());
    assertEquals("http://acme.org/p1", triples.get(0).getPredicateString());
    assertEquals("http://acme.org/o1", triples.get(0).getObjectString());

    assertEquals("http://acme.org/s2", triples.get(1).getSubjectString());
    assertEquals("http://acme.org/p2", triples.get(1).getPredicateString());
    assertEquals("http://acme.org/o2", triples.get(1).getObjectString());
  }

  @Test(expected=AnalyticsException.class)
  public void testNQuadsListWithMultipleContexts()
  throws Exception {
    final String nq = "<http://acme.org/s1> <http://acme.org/p1> <http://acme.org/o1> <http://acme.org/c1> .\n" +
                      "<http://acme.org/s2> <http://acme.org/p2> <http://acme.org/o2> <http://acme.org/c2> .";
    final TupleEntry te = new TupleEntry(new Tuple(nq));
    final JobConf conf = new JobConf();
    conf.set(AnalyticsParameters.DOCUMENT_FORMAT.toString(), DocumentFormat.NQUADS.toString());
    final RDFParser parser = new RDFParser(new HadoopFlowProcess(conf));
    parser.getRDFDocument(te);
  }

  @Test(expected=AnalyticsException.class)
  public void testNQuadsBadContext()
  throws Exception {
    final String nq = "<http://worldbank.270a.info/classification/project/P126357> "
                      + "<http://worldbank.270a.info/property/references> "
                      + "_:P126357references43 _:bnode1 .";
    final TupleEntry te = new TupleEntry(new Tuple(nq));
    final JobConf conf = new JobConf();
    conf.set(AnalyticsParameters.DOCUMENT_FORMAT.toString(), DocumentFormat.NQUADS.toString());
    final RDFParser parser = new RDFParser(new HadoopFlowProcess(conf));
    parser.getRDFDocument(te);
  }

  /**
   * GL-44
   * This N-Quads fails to parse because the context URI is not valid, i.e., it is not possible
   * to extract its second-level domain name using {@link RDFDocument#getSndDomain()}.
   */
  @Test(expected=AnalyticsException.class)
  public void testBadRDFDocument()
  throws Exception {
    final String nq = "<http://worldbank.270a.info/classification/project/P126357> "
                      + "<http://worldbank.270a.info/property/references> "
                      + "_:P126357references43 <http://context.rdf/> .";
    final TupleEntry te = new TupleEntry(new Tuple(nq));
    final JobConf conf = new JobConf();
    conf.set(AnalyticsParameters.DOCUMENT_FORMAT.toString(), DocumentFormat.NQUADS.toString());
    final RDFParser parser = new RDFParser(new HadoopFlowProcess(conf));
    parser.getRDFDocument(te);
  }

}
