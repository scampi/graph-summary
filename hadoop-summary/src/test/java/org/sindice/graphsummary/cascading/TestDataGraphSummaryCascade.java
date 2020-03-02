/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading;

import static org.sindice.graphsummary.cascading.SparqlHelper._assertQuery;
import static org.sindice.graphsummary.cascading.hash2value.rdf.DataGraphSummaryVocab.DOMAIN_URI_PREFIX;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.mapred.JobConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.repository.Repository;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
import org.sindice.core.analytics.cascading.CascadeConfYAMLoader;
import org.sindice.core.analytics.rdf.AnalyticsClassAttributes;
import org.sindice.core.analytics.rdf.DocumentFormat;
import org.sindice.graphsummary.AbstractSummaryTestCase;
import org.sindice.graphsummary.cascading.SparqlHelper.Pair;
import org.sindice.graphsummary.cascading.ecluster.ClusteringFactory.ClusteringAlgorithm;
import org.sindice.graphsummary.cascading.hash2value.rdf.RDFMetadataGraph;
import org.sindice.graphsummary.cascading.hash2value.rdf.RDFRelationsGraph;

import cascading.flow.hadoop.util.HadoopUtil;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tuple.Fields;

import com.google.common.collect.Lists;

public class TestDataGraphSummaryCascade
extends AbstractSummaryTestCase {

  private Repository repo;

  @Override
  @Before
  public void setUp()
  throws Exception {
    super.setUp();
    repo = null;
  }

  @Override
  @After
  public void tearDown()
  throws Exception {
    if (repo != null) {
      repo.shutDown();
    }
    super.tearDown();
  }

  /**
   * GL-116
   */
  @SuppressWarnings("serial")
  @Test
  public void testDatatypes()
  throws Exception {
    // Compute the summary
    final String input = "./src/test/resources/testDataGraphSummaryCascade/testDatatypes.nt";

    properties.setProperty(AnalyticsParameters.CHECK_AUTH_TYPE.toString(), "false");
    properties.setProperty(AnalyticsParameters.DOCUMENT_FORMAT.toString(), DocumentFormat.NTRIPLES.toString());
    properties.setProperty(SummaryParameters.DATATYPE.toString(), "true");

    final CascadeConfYAMLoader loader = new CascadeConfYAMLoader();
    loader.load(HadoopUtil.createJobConf(properties, new JobConf()), null);
    final Map<String, Properties> props = loader.getFlowsConfiguration(DataGraphSummaryCascade.FLOWS_NAME);

    DataGraphSummaryCascade
    .run(input, new TextLine(new Fields("value")), testOutput.getAbsolutePath(), props, ClusteringAlgorithm.PROPERTIES, false);
    // Get the RDF dumps and load them into a repository
    repo = SparqlHelper.createRepositoryFromQuads(getRDFDumps());

    // Query for the cluster properties
    final String q1 = "?edge an:source ?source .\n" +
                      "?edge an:label ?pLabel .\n" +
                      "?edge an:cardinality ?pCard .\n" +
                      "?edge an:datatype ?dt .\n" +
                      "?dt an:label ?dtLabel .\n" +
                      "?dt an:cardinality ?dtCard .\n";
    final List<List<Pair>> b1 = new ArrayList<List<Pair>>() {{
      add(
        Lists.newArrayList(
          new Pair("pLabel", "http://www.di.unipi.it/#produce"),
          new Pair("pCard", "2"),
          new Pair("dtLabel", "http://www.di.unipi.it/#beer"),
          new Pair("dtCard", "1")
      ));
      add(
        Lists.newArrayList(
          new Pair("pLabel", "http://www.di.unipi.it/#produce"),
          new Pair("pCard", "2"),
          new Pair("dtLabel", "http://www.di.unipi.it/#cheese"),
          new Pair("dtCard", "1")
      ));
    }};
    _assertQuery(q1, repo, b1, false);
  }

  @SuppressWarnings("serial")
  @Test
  public void testRDFDumps()
  throws Exception {
    final String config = "---\n" +
                          CascadeConfYAMLoader.DEFAULT_PARAMETERS + ":\n" +
                          "         - " + AnalyticsParameters.CLASS_ATTRIBUTES_FIELD.toString() + ":\n" +
                          "                  - http://www.w3.org/1999/02/22-rdf-syntax-ns#type\n" +
                          "                  - http://opengraphprotocol.org/schema/type\n" +
                          "                  - http://opengraph.org/schema/type\n" +
                          "                  - http://ogp.me/ns#type\n" +
                          "                  - http://purl.org/dc/elements/1.1/type\n" +
                          "                  - http://purl.org/stuff/rev#type\n" +
                          // Added 19 Oct 2011
                          "                  - http://purl.org/dc/terms/type\n" +
                          // Added 19 Oct 2011
                          "                  - http://dbpedia.org/property/type\n" +
                          "                  - http://dbpedia.org/ontology/type\n" +
                          "                  - http://dbpedia.org/ontology/Organisation/type\n" +
                          // Added 25 Oct 2011
                          "                  - http://xmlns.com/foaf/0.1/type\n" +
                          // Added 25 Oct 2011 +
                          "...\n";

    // Compute the summary
    final String input = "./src/test/resources/testDataGraphSummaryCascade/testDataGraphSummaryCascadeInput.json";
    final CascadeConfYAMLoader loader = new CascadeConfYAMLoader();
    loader.load(HadoopUtil.createJobConf(properties, new JobConf()), new ByteArrayInputStream(config.getBytes()));
    final Map<String, Properties> props = loader.getFlowsConfiguration(DataGraphSummaryCascade.FLOWS_NAME);

    DataGraphSummaryCascade
    .run(input, new TextLine(new Fields("value")), testOutput.getAbsolutePath(), props, ClusteringAlgorithm.TYPES, false);
    // Get the RDF dumps and load them into a repository
    repo = SparqlHelper.createRepositoryFromQuads(getRDFDumps());

    // Query for the cluster properties
    final String q1 = "?cluster any23:domain ?Domain;" +
                               "any23:domain_uri ?DomainUri;" +
                               "an:cardinality ?ClusterCardinality;" +
                               "an:label ?l .\n" +
                      "?l an:type ?t .\n" +
                      "?l an:label ?Label .\n" +
                      "?t an:label ?Type; an:cardinality ?TypeCardinality .\n";
    final List<List<Pair>> b1 = new ArrayList<List<Pair>>() {{
      add(
        Lists.newArrayList(
          new Pair("Domain", "countries.eu"),
          new Pair("DomainUri", DOMAIN_URI_PREFIX + "countries.eu"),
          new Pair("ClusterCardinality", "1"),
          new Pair("Label", "http://www.countries.eu/beer"),
          new Pair("Type", AnalyticsClassAttributes.DEFAULT_CLASS_ATTRIBUTE),
          new Pair("TypeCardinality", "1")
      ));
      add(
        Lists.newArrayList(
          new Pair("Domain", "countries.eu"),
          new Pair("DomainUri", DOMAIN_URI_PREFIX + "countries.eu"),
          new Pair("ClusterCardinality", "1"),
          new Pair("Label", "country"),
          new Pair("Type", "http://ogp.me/ns#type"),
          new Pair("TypeCardinality", "1")
      ));
      add(
        Lists.newArrayList(
          new Pair("Domain", "unipi.it"),
          new Pair("DomainUri", DOMAIN_URI_PREFIX + "unipi.it"),
          new Pair("ClusterCardinality", "1"),
          new Pair("Label", "http://www.countries.eu/person"),
          new Pair("Type", AnalyticsClassAttributes.DEFAULT_CLASS_ATTRIBUTE),
          new Pair("TypeCardinality", "1")
      ));
      add(
        Lists.newArrayList(
          new Pair("Domain", "unipi.it"),
          new Pair("DomainUri", DOMAIN_URI_PREFIX + "unipi.it"),
          new Pair("ClusterCardinality", "1"),
          new Pair("Label", "http://www.countries.eu/beer"),
          new Pair("Type", AnalyticsClassAttributes.DEFAULT_CLASS_ATTRIBUTE),
          new Pair("TypeCardinality", "1")
      ));
      add(
        Lists.newArrayList(
          new Pair("Domain", "unipi.it"),
          new Pair("DomainUri", DOMAIN_URI_PREFIX + "unipi.it"),
          new Pair("ClusterCardinality", "1"),
          new Pair("Label", "http://www.countries.eu/drink"),
          new Pair("Type", AnalyticsClassAttributes.DEFAULT_CLASS_ATTRIBUTE),
          new Pair("TypeCardinality", "1")
      ));
      add(
        Lists.newArrayList(
          new Pair("Domain", "unipi.it"),
          new Pair("DomainUri", DOMAIN_URI_PREFIX + "unipi.it"),
          new Pair("ClusterCardinality", "1"),
          new Pair("Label", "http://www.countries.eu/drink"),
          new Pair("Type", AnalyticsClassAttributes.DEFAULT_CLASS_ATTRIBUTE),
          new Pair("TypeCardinality", "1")
      ));
    }};
    _assertQuery(q1, repo, b1, false);
    // Query for the cluster relations between clusters
    final String q2 = "?src any23:domain ?SourceDomain;" +
                           "an:label ?ls .\n" +
                      "?ls an:label ?SourceLabel .\n" +
                      "?target any23:domain ?TargetDomain;" +
                              "an:label ?lt .\n" +
                      "?lt an:label ?TargetLabel .\n" +
                      "?edge an:source ?src;" +
                            "an:target ?target;" +
                            "an:publishedIn ?publishedIn;" +
                            "an:cardinality ?EdgeCardinality;" +
                            "an:label ?EdgeLabel";
    final List<List<Pair>> b2 = new ArrayList<List<Pair>>() {{
      add(
        Lists.newArrayList(
          new Pair("SourceDomain", "countries.eu"),
          new Pair("SourceLabel", "country"),
          new Pair("TargetDomain", "countries.eu"),
          new Pair("TargetLabel", "http://www.countries.eu/beer"),
          new Pair("EdgeCardinality", "1"),
          new Pair("EdgeLabel", "http://www.di.unipi.it/#produce"),
          new Pair("publishedIn", DOMAIN_URI_PREFIX + "countries.eu")
      ));
      add(
        Lists.newArrayList(
          new Pair("SourceDomain", "unipi.it"),
          new Pair("SourceLabel", "http://www.countries.eu/person"),
          new Pair("TargetDomain", "countries.eu"),
          new Pair("TargetLabel", "country"),
          new Pair("EdgeCardinality", "1"),
          new Pair("EdgeLabel", "http://www.di.unipi.it/#livein"),
          new Pair("publishedIn", DOMAIN_URI_PREFIX + "unipi.it")
      ));
      add(
        Lists.newArrayList(
          new Pair("SourceDomain", "unipi.it"),
          new Pair("SourceLabel", "http://www.countries.eu/person"),
          new Pair("TargetDomain", "unipi.it"),
          new Pair("TargetLabel", "http://www.countries.eu/beer"),
          new Pair("EdgeCardinality", "1"),
          new Pair("EdgeLabel", "http://www.di.unipi.it/#produce"),
          new Pair("publishedIn", DOMAIN_URI_PREFIX + "unipi.it")
      ));
      add(
        Lists.newArrayList(
          new Pair("SourceDomain", "unipi.it"),
          new Pair("SourceLabel", "http://www.countries.eu/person"),
          new Pair("TargetDomain", "unipi.it"),
          new Pair("TargetLabel", "http://www.countries.eu/drink"),
          new Pair("EdgeCardinality", "1"),
          new Pair("EdgeLabel", "http://www.di.unipi.it/#produce"),
          new Pair("publishedIn", DOMAIN_URI_PREFIX + "unipi.it")
      ));
    }};
    _assertQuery(q2, repo, b2, false);
  }

  /**
   * This method retrieve the RDF dumps of the summary computed by
   * {@link RDFRelationsGraph} and {@link RDFMetadataGraph}. The sink {@link Scheme}
   * is defined in {@link DataGraphSummaryCascade}.
   * @return a {@link List} of quads.
   */
  private List<String> getRDFDumps()
  throws Exception {
    final File[] relationDumps = new File(testOutput, "relations-graph-rdf")
    .listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".nq");
      }
    });
    final File[] metadataDumps = new File(testOutput, "metadata-graph-rdf")
    .listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".nq");
      }
    });

    final List<String> quads = new ArrayList<String>();
    for (File file : relationDumps) {
      String quad = "";
      final BufferedReader r = new BufferedReader(new FileReader(file));
      try {
        while ((quad = r.readLine()) != null) {
          quads.add(quad);
        }
      } finally {
        r.close();
      }
    }
    for (File file : metadataDumps) {
      String quad = "";
      final BufferedReader r = new BufferedReader(new FileReader(file));
      try {
        while ((quad = r.readLine()) != null) {
          quads.add(quad);
        }
      } finally {
        r.close();
      }
    }
    return quads;
  }

}
