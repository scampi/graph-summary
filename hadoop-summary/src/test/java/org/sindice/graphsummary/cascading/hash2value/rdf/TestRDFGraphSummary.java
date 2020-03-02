/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.hash2value.rdf;

import static org.sindice.graphsummary.cascading.SparqlHelper._assertQuery;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.repository.Repository;
import org.openrdf.rio.ntriples.NTriplesUtil;
import org.openrdf.sail.memory.model.MemValueFactory;
import org.sindice.analytics.entity.AnalyticsValue;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.rdf.SummaryContext;
import org.sindice.core.analytics.testHelper.CheckScheme;
import org.sindice.core.analytics.testHelper.iotransformation.ConvertFunction;
import org.sindice.core.analytics.testHelper.iotransformation.LongType;
import org.sindice.core.analytics.testHelper.iotransformation.UnitOfWorkTestHelper;
import org.sindice.core.analytics.util.Hash;
import org.sindice.graphsummary.AbstractSummaryTestCase;
import org.sindice.graphsummary.cascading.SparqlHelper;
import org.sindice.graphsummary.cascading.SparqlHelper.Pair;
import org.sindice.graphsummary.cascading.hash2value.GenerateDictionary;
import org.sindice.graphsummary.cascading.properties.GetPropertiesGraph;
import org.sindice.graphsummary.cascading.properties.GetPropertiesGraph.PropertiesProcessing;
import org.sindice.graphsummary.iotransformation.AnalyticsValueType;
import org.sindice.graphsummary.iotransformation.EntityDescriptionType;
import org.sindice.graphsummary.iotransformation.Hash128Type;
import org.sindice.graphsummary.iotransformation.Hash64Type;
import org.sindice.graphsummary.iotransformation.PropertiesCountType;
import org.sindice.graphsummary.iotransformation.SortedListToHash128Type;
import org.sindice.graphsummary.iotransformation.TypesCountType;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

import com.google.common.collect.Lists;

@SuppressWarnings("serial")
public class TestRDFGraphSummary extends AbstractSummaryTestCase {

  private Repository repo;

  /**
   * Creates the dictionaries that match an hash value with its String original representation,
   * in order to convert hashed values in the graph summary output.
   * <p>
   * The data is stored in files ending with "dictionary.check", one for each dictionary.
   * Each value is N-Triple formatted.
   * @param folder the folder with the <b>*-dictionary.txt</b> file.
   * @throws IOException 
   */
  private void createDictionaries(final String folder)
  throws IOException {
    final Class[] types = { Hash64Type.class, AnalyticsValueType.class };
    final String domainInput = "src/test/resources/testRDFGraphSummary/" + folder + "/domain-dictionary.check";
    final String typeInput = "src/test/resources/testRDFGraphSummary/" + folder + "/type-dictionary.check";
    final String predicateInput = "src/test/resources/testRDFGraphSummary/" + folder + "/predicate-dictionary.check";
    final String datatypeInput = "src/test/resources/testRDFGraphSummary/" + folder + "/datatype-dictionary.check";

    properties.setProperty(AnalyticsParameters.HFILE_PREFIX.toString(), GenerateDictionary.DOMAIN_PREFIX);
    final String domainDict = UnitOfWorkTestHelper.createHFile(properties, types, domainInput, testOutput);

    properties.setProperty(AnalyticsParameters.HFILE_PREFIX.toString(), GenerateDictionary.CLASS_PREFIX);
    final String typeDict = UnitOfWorkTestHelper.createHFile(properties, types, typeInput, testOutput);

    properties.setProperty(AnalyticsParameters.HFILE_PREFIX.toString(), GenerateDictionary.PREDICATE_PREFIX);
    final String predicateDict = UnitOfWorkTestHelper.createHFile(properties, types, predicateInput, testOutput);

    properties.setProperty(AnalyticsParameters.HFILE_PREFIX.toString(), GenerateDictionary.DATATYPE_PREFIX);
    final String datatypeDict = UnitOfWorkTestHelper.createHFile(properties, types, datatypeInput, testOutput);

    // put the dictionary into the configuration
    properties.setProperty(GenerateDictionary.CLASS_PREFIX, typeDict);
    properties.setProperty(GenerateDictionary.PREDICATE_PREFIX, predicateDict);
    properties.setProperty(GenerateDictionary.DOMAIN_PREFIX, domainDict);
    properties.setProperty(GenerateDictionary.DATATYPE_PREFIX, datatypeDict);
  }

  /**
   * Compute hash-value pairs, for each element separated by a comma,
   * and check that the dictionary data is valid.
   * @param tuple the dictionary data
   * @param lineNb the line offset in the dictionary
   * @param hashes the list of pairs
   */
  private void getPairs(String tuple, final int lineNb, final List<Object> hashes) {
    final String dictName;

    switch (lineNb) {
      case 0:
        dictName = "domain";
        break;
      case 1:
        dictName = "type";
        break;
      case 2:
        dictName = "predicate";
        break;
      case 3:
        dictName = "datatype";
        break;
      default:
        throw new IllegalArgumentException("Invalid dictionary entry at line [" + (lineNb + 1) + "]");
    }

    for (String value : tuple.split(",")) {
      final AnalyticsValue v = AnalyticsValue.fromSesame(NTriplesUtil.parseValue(value, new MemValueFactory()));
      hashes.add(Hash.getHash64(v.getValue()));
      hashes.add(v);
    }

    if (hashes == null || hashes.size() % 2 != 0) {
      throw new RuntimeException("Problem with the values mapping of the [" + dictName + "] dictionary." +
          " It must be an array of paired elements: " + hashes);
    }
  }

  @Override
  @Before
  public void setUp()
  throws Exception {
    super.setUp();
    properties.setProperty(AnalyticsParameters.CLASS_ATTRIBUTES_FIELD.toString(),
        "http://opengraph.org/schema/type,"
        + "http://ogp.me/ns#type,"
        + "http://purl.org/dc/terms/type,"
        + "http://xmlns.com/foaf/0.1/type"
    );
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

  private void getSummaryQuads(final List<String> quads,
                               final String sourceTuples,
                               final Class<? extends AnalyticsSubAssembly> assembly,
                               final Fields fieldsInput,
                               final Class[] fieldsTypes)
  throws Exception {
    final FlowConnector rdf = new HadoopFlowConnector(properties);
    final Hfs source = new Hfs(new CheckScheme(Analytics.getHeadFields(assembly)), sourceTuples);
    final Hfs sink = new Hfs(new SequenceFile(new Fields("rdf")),
      testOutput.getAbsolutePath() + "/" + assembly.getSimpleName());

    Pipe pipe = new Each("RDF", new ConvertFunction(fieldsInput, fieldsTypes));
    try {
      final Constructor<?> constructor = assembly.getConstructor(Pipe[].class);
      pipe = (Pipe) constructor.newInstance((Object) Pipe.pipes(pipe));
    } catch (Exception e) {
      throw new RuntimeException("Exception catched", e);
    }

    final Flow flow = rdf.connect("RDF", source, sink, pipe);
    flow.complete();

    final TupleEntryIterator it = flow.openSink();
    // put the triples into the repository
    try {
      while (it.hasNext()) {
        final TupleEntry tuple = it.next();
        quads.add(tuple.getString("rdf"));
      }
    } finally {
      it.close();
    }
  }

  @Test
  public void testBNode() throws Exception {
    final Class<?>[] relationsTypes = { Hash64Type.class, Hash64Type.class, SortedListToHash128Type.class,
        Hash64Type.class, Hash64Type.class, SortedListToHash128Type.class, LongType.class };
    final Class<?>[] propertiesTypes = { Hash64Type.class, SortedListToHash128Type.class, TypesCountType.class,
        PropertiesCountType.class, LongType.class };

    createDictionaries("");

    final List<String> quads = new ArrayList<String>();
    getSummaryQuads(quads, "src/test/resources/testRDFGraphSummary/testRelationsGraphInput.check",
      RDFRelationsGraph.class, Analytics.getHeadFields(RDFRelationsGraph.class), relationsTypes);
    getSummaryQuads(quads, "src/test/resources/testRDFGraphSummary/testPropertiesGraphInput.check",
      RDFMetadataGraph.class, Analytics.getHeadFields(RDFMetadataGraph.class), propertiesTypes);
    repo = SparqlHelper.createRepositoryFromQuads(quads);

    final String q1 = "?s ?p ?o FILTER(isBlank(?s) || isBlank(?p) || isBlank(?o))";
    final List<List<Pair>> b1 = new ArrayList<List<Pair>>();
    _assertQuery(q1, repo, b1, false);
  }

  @Test
  public void testGraphSummary() throws Exception {
    final Class<?>[] relationsTypes = { Hash64Type.class, Hash64Type.class, SortedListToHash128Type.class,
        Hash64Type.class, Hash64Type.class, SortedListToHash128Type.class, LongType.class };
    final Class<?>[] propertiesTypes = { Hash64Type.class, SortedListToHash128Type.class, TypesCountType.class,
        PropertiesCountType.class, LongType.class };

    createDictionaries("");

    final List<String> quads = new ArrayList<String>();
    getSummaryQuads(quads, "src/test/resources/testRDFGraphSummary/testRelationsGraphInput.check",
      RDFRelationsGraph.class, Analytics.getHeadFields(RDFRelationsGraph.class), relationsTypes);
    getSummaryQuads(quads, "src/test/resources/testRDFGraphSummary/testPropertiesGraphInput.check",
      RDFMetadataGraph.class, Analytics.getHeadFields(RDFMetadataGraph.class), propertiesTypes);
    repo = SparqlHelper.createRepositoryFromQuads(quads);

    final long nodeHash = Hash.getHash64("D2");
    final String clusterHash = Hash.getHash128("C1").toString().replace(" ", "");
    final String node = DataGraphSummaryVocab.ANY23_PREFIX + "node"
        + Long.toString(nodeHash).replace('-', 'n') + clusterHash;

    final String q1 = " <" + node + "> any23:domain \"D2\" ." +
                      " <" + node + "> an:label ?bnode ." +
                      " ?bnode an:label ?label .";
    final List<List<Pair>> b1 = new ArrayList<List<Pair>>() {{
      add(Lists.newArrayList(new Pair("label", "http://xmlns.com/foaf/0.1/Person")));
      add(Lists.newArrayList(new Pair("label", "http://xmlns.com/foaf/0.1/Student")));
    }};
    _assertQuery(q1, repo, b1, false);

    final String q2 = "?node any23:domain \"D2\" ." + " ?node an:label ?bnode ."
                      + "?bnode an:label ?label .";
    final List<List<Pair>> b2 = new ArrayList<List<Pair>>() {
      {
        add(Lists
        .newArrayList(new Pair("label", "bla-bla")));
        add(Lists
        .newArrayList(new Pair("label", "bla-bla")));
        add(Lists
        .newArrayList(new Pair("label", "http://xmlns.com/foaf/0.1/Person")));
        add(Lists
        .newArrayList(new Pair("label", "http://xmlns.com/foaf/0.1/Professor")));
        add(Lists
        .newArrayList(new Pair("label", "http://xmlns.com/foaf/0.1/Student")));
        add(Lists
        .newArrayList(new Pair("label", "http://xmlns.com/foaf/0.1/Student")));
      }
    };
    _assertQuery(q2, repo, b2, false);

    // query for the global_id
    final String global = DataGraphSummaryVocab.ANY23_PREFIX + "global" + clusterHash;
    final String q3 = "?node any23:domain ?domain .\n" +
                      "?node an:global_id <" + global + "> .\n";
    final List<List<Pair>> b3 = new ArrayList<List<Pair>>() {
      {
        add(Lists.newArrayList(new Pair("domain", "D2")));
      }
    };
    _assertQuery(q3, repo, b3, false);

    // query for the edge labelled <http://xmlns.com/foaf/0.1/knows>
    final String q4 =   "?srcNode any23:domain_uri ?srcDomain .\n"
                      + "?targetNode any23:domain_uri ?targetDomain .\n"
                      + "?edge an:source ?srcNode .\n"
                      + "?edge an:target ?targetNode .\n"
                      + "?edge an:publishedIn ?publishDomain .\n"
                      + "?edge an:label <http://xmlns.com/foaf/0.1/knows> .\n"
                      + "?edge an:cardinality ?card .";
    final List<List<Pair>> b4 = new ArrayList<List<Pair>>() {
      {
        add(Lists.newArrayList(
          new Pair("srcDomain", DataGraphSummaryVocab.DOMAIN_URI_PREFIX + "D2"),
          new Pair("targetDomain", DataGraphSummaryVocab.DOMAIN_URI_PREFIX + "D1"),
          new Pair("publishDomain", DataGraphSummaryVocab.DOMAIN_URI_PREFIX + "D1"),
          new Pair("card", "1")
        ));
        add(Lists.newArrayList(
          new Pair("srcDomain", DataGraphSummaryVocab.DOMAIN_URI_PREFIX + "D2"),
          new Pair("targetDomain", DataGraphSummaryVocab.DOMAIN_URI_PREFIX + "D2"),
          new Pair("publishDomain", DataGraphSummaryVocab.DOMAIN_URI_PREFIX + "D2"),
          new Pair("card", "12")
        ));
        add(Lists.newArrayList(
          new Pair("srcDomain", DataGraphSummaryVocab.DOMAIN_URI_PREFIX + "D2"),
          new Pair("targetDomain", DataGraphSummaryVocab.DOMAIN_URI_PREFIX + "D2"),
          new Pair("publishDomain", DataGraphSummaryVocab.DOMAIN_URI_PREFIX + "D2"),
          new Pair("card", "13")
        ));
        add(Lists.newArrayList(
          new Pair("srcDomain", DataGraphSummaryVocab.DOMAIN_URI_PREFIX + "D3"),
          new Pair("targetDomain", DataGraphSummaryVocab.DOMAIN_URI_PREFIX + "D2"),
          new Pair("publishDomain", DataGraphSummaryVocab.DOMAIN_URI_PREFIX + "D3"),
          new Pair("card", "21")
        ));
      }
    };
    _assertQuery(q4, repo, b4, false);
  }

  @Test
  public void testMetadata()
  throws Exception {
    final Class<?>[] relationsTypes = { Hash64Type.class, Hash64Type.class, SortedListToHash128Type.class,
        Hash64Type.class, Hash64Type.class, SortedListToHash128Type.class, LongType.class };
    final Class<?>[] propertiesTypes = { Hash64Type.class, SortedListToHash128Type.class, TypesCountType.class,
        PropertiesCountType.class, LongType.class };

    createDictionaries("");

    final List<String> quads = new ArrayList<String>();
    getSummaryQuads(quads, "src/test/resources/testRDFGraphSummary/testRelationsGraphInput.check",
      RDFRelationsGraph.class, Analytics.getHeadFields(RDFRelationsGraph.class), relationsTypes);
    getSummaryQuads(quads, "src/test/resources/testRDFGraphSummary/testPropertiesGraphInput.check",
      RDFMetadataGraph.class, Analytics.getHeadFields(RDFMetadataGraph.class), propertiesTypes);
    repo = SparqlHelper.createRepositoryFromQuads(quads);

    final String domain = DataGraphSummaryVocab.DOMAIN_URI_PREFIX + "D2";

    // Query for cardinality
    final String q1 = "?srcNode any23:domain_uri <" + domain + "> .\n" +
                      "?srcNode an:cardinality ?nodeCard ." +
                      "?srcNode an:label ?l ." +
                      "?l an:label ?nodeLabel";
    final List<List<Pair>> b1 = new ArrayList<List<Pair>>() {
      {
        add(Lists.newArrayList(
          new Pair("nodeLabel", "http://xmlns.com/foaf/0.1/Person"),
          new Pair("nodeCard", "2")
        ));
        add(Lists.newArrayList(
          new Pair("nodeLabel", "http://xmlns.com/foaf/0.1/Student"),
          new Pair("nodeCard", "2")
        ));
        add(Lists.newArrayList(
          new Pair("nodeLabel", "bla-bla"),
          new Pair("nodeCard", "3")
        ));
        add(Lists.newArrayList(
          new Pair("nodeLabel", "http://xmlns.com/foaf/0.1/Student"),
          new Pair("nodeCard", "3")
        ));
        add(Lists.newArrayList(
          new Pair("nodeLabel", "bla-bla"),
          new Pair("nodeCard", "1")
        ));
        add(Lists.newArrayList(
          new Pair("nodeLabel", "http://xmlns.com/foaf/0.1/Professor"),
          new Pair("nodeCard", "1")
        ));
      }
    };
    _assertQuery(q1, repo, b1, false);

    // Query for properties as edges
    final String q2 = "?srcNode any23:domain_uri <" + domain + "> .\n" +
                      "?edge an:source ?srcNode ." +
                      "?edge an:target \"" + DataGraphSummaryVocab.BLANK_NODE_COLLECTION + "\" ." +
                      "?edge an:label ?propLabel ." +
                      "?edge an:cardinality ?propCard ." +
                      "?edge an:publishedIn ?publishedIn .";
    final List<List<Pair>> b2 = new ArrayList<List<Pair>>() {
      {
        add(Lists.newArrayList(
          new Pair("propLabel", "http://xmlns.com/foaf/0.1/knows"),
          new Pair("propCard", "1"),
          new Pair("publishedIn", domain)
        ));
        add(Lists.newArrayList(
          new Pair("propLabel", "http://xmlns.com/foaf/0.1/name"),
          new Pair("propCard", "19"),
          new Pair("publishedIn", domain)
        ));
        add(Lists.newArrayList(
          new Pair("propLabel", "http://xmlns.com/foaf/0.1/age"),
          new Pair("propCard", "10"),
          new Pair("publishedIn", domain)
        ));
      }
    };
    _assertQuery(q2, repo, b2, false);

    // Query for the class Attributes URIs
    final String q3 = " ?srcNode any23:domain_uri <" + domain + "> .\n" +
                      " ?srcNode an:label ?l ." +
                      " ?l an:label ?label ." +
                      " ?l an:type ?t ." +
                      " ?t an:label ?type ." +
                      " ?t an:cardinality ?typeCard .";
    final List<List<Pair>> b3 = new ArrayList<List<Pair>>() {
      {
        add(Lists.newArrayList(
          new Pair("label", "http://xmlns.com/foaf/0.1/Person"),
          new Pair("type", "http://ogp.me/ns#type"),
          new Pair("typeCard", "10")
        ));
        add(Lists.newArrayList(
          new Pair("label", "http://xmlns.com/foaf/0.1/Person"),
          new Pair("type", "http://xmlns.com/foaf/0.1/type"),
          new Pair("typeCard", "2")
        ));
        add(Lists.newArrayList(
          new Pair("label", "http://xmlns.com/foaf/0.1/Student"),
          new Pair("type", "http://opengraph.org/schema/type"),
          new Pair("typeCard", "23")
        ));
        add(Lists.newArrayList(
          new Pair("label", "bla-bla"),
          new Pair("type", "http://xmlns.com/foaf/0.1/type"),
          new Pair("typeCard", "10")
        ));
        add(Lists.newArrayList(
          new Pair("label", "http://xmlns.com/foaf/0.1/Student"),
          new Pair("type", "http://purl.org/dc/terms/type"),
          new Pair("typeCard", "10")
        ));
        add(Lists.newArrayList(
          new Pair("label", "bla-bla"),
          new Pair("type", "http://ogp.me/ns#type"),
          new Pair("typeCard", "10")
        ));
        add(Lists.newArrayList(
          new Pair("label", "http://xmlns.com/foaf/0.1/Professor"),
          new Pair("type", "http://xmlns.com/foaf/0.1/type"),
          new Pair("typeCard", "2")
        ));
      }
    };
    _assertQuery(q3, repo, b3, false);
  }

  @Test
  public void testPropertiesWithSpaceGraph()
  throws Exception {
    final Class<?>[] propertiesTypes = { Hash64Type.class, SortedListToHash128Type.class, TypesCountType.class,
        PropertiesCountType.class, LongType.class };

    createDictionaries("testPropertiesWithSpaceGraph");

    final List<String> quads = new ArrayList<String>();
    getSummaryQuads(quads, "src/test/resources/testRDFGraphSummary/" +
        "testPropertiesWithSpaceGraph/testPropertiesWithSpaceGraphInput.check",
      RDFMetadataGraph.class, Analytics.getHeadFields(RDFMetadataGraph.class), propertiesTypes);
    repo = SparqlHelper.createRepositoryFromQuads(quads);

    final String q1 = "?edge an:publishedIn <" + DataGraphSummaryVocab.DOMAIN_URI_PREFIX + "DSPACE> ." +
                      "?edge an:label ?label ." +
                      "?edge an:cardinality ?card .";
    final List<List<Pair>> b1 = new ArrayList<List<Pair>>() {
      {
        add(Lists.newArrayList(
          new Pair("label", "http://xmlns.com/foaf/0.1/sp ace"),
          new Pair("card", "10")
        ));
      }
    };
    _assertQuery(q1, repo, b1, false);
  }

  /**
   * @see AnalyticsParameters#DATASET_URI
   */
  @Test
  public void testDatasetUri()
  throws Exception {
    final Class<?>[] relationsTypes = { Hash64Type.class, Hash64Type.class, SortedListToHash128Type.class,
        Hash64Type.class, Hash64Type.class, SortedListToHash128Type.class, LongType.class };
    final Class<?>[] propertiesTypes = { Hash64Type.class, SortedListToHash128Type.class, TypesCountType.class,
        PropertiesCountType.class, LongType.class };

    createDictionaries("");

    final String datasetUri = "http://acme.org";
    properties.setProperty(AnalyticsParameters.DATASET_URI.toString(), datasetUri);

    final List<String> quads = new ArrayList<String>();
    getSummaryQuads(quads, "src/test/resources/testRDFGraphSummary/testRelationsGraphInput.check", RDFRelationsGraph.class,
      Analytics.getHeadFields(RDFRelationsGraph.class), relationsTypes);
    getSummaryQuads(quads, "src/test/resources/testRDFGraphSummary/testPropertiesGraphInput.check", RDFMetadataGraph.class,
      Analytics.getHeadFields(RDFMetadataGraph.class), propertiesTypes);
    repo = SparqlHelper.createRepositoryFromQuads(quads);

    // Check {@link DataGraphSummaryVocab#EDGE_PUBLISHED_IN} property
    final String q1 = "?edge an:publishedIn ?publishedIn .";
    final List<List<Pair>> b1 = new ArrayList<List<Pair>>() {
      {
        add(Lists.newArrayList(
          new Pair("publishedIn", datasetUri)
        ));
      }
    };
    _assertQuery(q1, repo, b1, true);

    // Check {@link DataGraphSummaryVocab#DOMAIN_URI} property
    final String q2 = "?edge any23:domain_uri ?domain_uri .";
    final List<List<Pair>> b2 = new ArrayList<List<Pair>>() {
      {
        add(Lists.newArrayList(
          new Pair("domain_uri", datasetUri)
        ));
      }
    };
    _assertQuery(q2, repo, b2, true);

    // Check {@link DataGraphSummaryVocab#DOMAIN_NAME} property
    final String q3 = "?edge any23:domain ?domain_name .";
    final List<List<Pair>> b3 = new ArrayList<List<Pair>>() {
      {
        add(Lists.newArrayList(
          new Pair("domain_name", datasetUri)
        ));
      }
    };
    _assertQuery(q3, repo, b3, true);
  }

  /**
   * @see AnalyticsParameters#CONTEXT
   */
  @Test
  public void testSummaryContext1()
  throws Exception {
    final Class<?>[] relationsTypes = { Hash64Type.class, Hash64Type.class, SortedListToHash128Type.class,
        Hash64Type.class, Hash64Type.class, SortedListToHash128Type.class, LongType.class };
    final Class<?>[] propertiesTypes = { Hash64Type.class, SortedListToHash128Type.class, TypesCountType.class,
        PropertiesCountType.class, LongType.class };

    createDictionaries("");

    properties.setProperty(AnalyticsParameters.CONTEXT.toString(), SummaryContext.DATASET.toString());

    final List<String> quads = new ArrayList<String>();
    getSummaryQuads(quads, "src/test/resources/testRDFGraphSummary/testRelationsGraphInput.check", RDFRelationsGraph.class,
      Analytics.getHeadFields(RDFRelationsGraph.class), relationsTypes);
    getSummaryQuads(quads, "src/test/resources/testRDFGraphSummary/testPropertiesGraphInput.check", RDFMetadataGraph.class,
      Analytics.getHeadFields(RDFMetadataGraph.class), propertiesTypes);
    repo = SparqlHelper.createRepositoryFromQuads(quads);

    final String q1 = "GRAPH ?g { ?s ?p ?o }";
    final List<List<Pair>> b1 = new ArrayList<List<Pair>>() {
      {
        add(Lists.newArrayList(
          new Pair("g", DataGraphSummaryVocab.DOMAIN_URI_PREFIX + "D1")
        ));
        add(Lists.newArrayList(
          new Pair("g", DataGraphSummaryVocab.DOMAIN_URI_PREFIX + "D2")
        ));
        add(Lists.newArrayList(
          new Pair("g", DataGraphSummaryVocab.DOMAIN_URI_PREFIX + "D3")
        ));
      }
    };
    _assertQuery(q1, repo, b1, true);

    // Check the D1 dataset
    final String q2 = "GRAPH <" + DataGraphSummaryVocab.DOMAIN_URI_PREFIX + "D1> {" +
                        "?s <" + DataGraphSummaryVocab.EDGE_SOURCE + "> ?source;" +
                        "   <" + DataGraphSummaryVocab.EDGE_TARGET + "> ?target;" +
                        "   <" + DataGraphSummaryVocab.LABEL + "> ?label;" +
                        "   <" + DataGraphSummaryVocab.CARDINALITY + "> ?card" +
                      "}";
    final List<List<Pair>> b2 = new ArrayList<List<Pair>>() {
      {
        add(Lists.newArrayList(
          new Pair("label", "http://xmlns.com/foaf/0.1/knows"),
          new Pair("card", "1")
        ));
        add(Lists.newArrayList(
          new Pair("label", "http://xmlns.com/foaf/0.1/age"),
          new Pair("card", "10")
        ));
      }
    };
    _assertQuery(q2, repo, b2, false);

    // Check the D2 dataset
    final String q3 = "GRAPH <" + DataGraphSummaryVocab.DOMAIN_URI_PREFIX + "D2> {" +
                        "?s <" + DataGraphSummaryVocab.LABEL + "> ?l." +
                        "?l <" + DataGraphSummaryVocab.LABEL + "> ?label" +
                      "}";
    final List<List<Pair>> b3 = new ArrayList<List<Pair>>() {
    {
      add(Lists.newArrayList(
        new Pair("label", "http://xmlns.com/foaf/0.1/Person")
      ));
      add(Lists.newArrayList(
        new Pair("label", "http://xmlns.com/foaf/0.1/Student")
      ));
      add(Lists.newArrayList(
        new Pair("label", "bla-bla")
      ));
      add(Lists.newArrayList(
        new Pair("label", "http://xmlns.com/foaf/0.1/Student")
      ));
      add(Lists.newArrayList(
        new Pair("label", "bla-bla")
      ));
      add(Lists.newArrayList(
        new Pair("label", "http://xmlns.com/foaf/0.1/Professor")
      ));
      }
    };
    _assertQuery(q3, repo, b3, false);
  }

  /**
   * With default parameter value
   * @see AnalyticsParameters#CONTEXT
   */
  @Test
  public void testSummaryContext2()
  throws Exception {
    final Class<?>[] relationsTypes = { Hash64Type.class, Hash64Type.class, SortedListToHash128Type.class,
        Hash64Type.class, Hash64Type.class, SortedListToHash128Type.class, LongType.class };
    final Class<?>[] propertiesTypes = { Hash64Type.class, SortedListToHash128Type.class, TypesCountType.class,
        PropertiesCountType.class, LongType.class };

    createDictionaries("");

    final List<String> quads = new ArrayList<String>();
    getSummaryQuads(quads, "src/test/resources/testRDFGraphSummary/testRelationsGraphInput.check", RDFRelationsGraph.class,
      Analytics.getHeadFields(RDFRelationsGraph.class), relationsTypes);
    getSummaryQuads(quads, "src/test/resources/testRDFGraphSummary/testPropertiesGraphInput.check", RDFMetadataGraph.class,
      Analytics.getHeadFields(RDFMetadataGraph.class), propertiesTypes);
    repo = SparqlHelper.createRepositoryFromQuads(quads);

    final String q1 = "GRAPH ?g { ?s ?p ?o }";
    final List<List<Pair>> b1 = new ArrayList<List<Pair>>() {
      {
        add(Lists.newArrayList(
          new Pair("g", DataGraphSummaryVocab.GRAPH_SUMMARY_GRAPH)
        ));
      }
    };
    _assertQuery(q1, repo, b1, true);
  }

  /**
   * Bug GL-15
   */
  @Test
  public void testEdgeResourceID()
  throws Exception {
    final Class<?>[] relationsTypes = { Hash64Type.class, Hash64Type.class, SortedListToHash128Type.class,
        Hash64Type.class, Hash64Type.class, SortedListToHash128Type.class, LongType.class };

    createDictionaries("testEdgeResourceID");

    final List<String> quads = new ArrayList<String>();
    getSummaryQuads(quads, "src/test/resources/testRDFGraphSummary/testEdgeResourceID/testRelationsGraphInput.check",
      RDFRelationsGraph.class, Analytics.getHeadFields(RDFRelationsGraph.class), relationsTypes);
    repo = SparqlHelper.createRepositoryFromQuads(quads);

    final String q1 = "?edge an:publishedIn <" + DataGraphSummaryVocab.DOMAIN_URI_PREFIX + "D3>;" +
                      "      an:cardinality ?card";
    final List<List<Pair>> b1 = new ArrayList<List<Pair>>() {
      {
        add(Lists.newArrayList(
          new Pair("card", "13")
        ));
      }
    };
    _assertQuery(q1, repo, b1, true);
  }

  /**
   * GL-98
   */
  @Test
  public void testSingleType()
  throws Exception {
    final String input = "./src/test/resources/testRDFGraphSummary/testSingleType/input.check";
    final String output = "./src/test/resources/testRDFGraphSummary/testSingleType/output.check";

    Class<?>[] typesInput = { Hash64Type.class, Hash128Type.class, EntityDescriptionType.class,
        EntityDescriptionType.class, SortedListToHash128Type.class };
    Class<?>[] typesOutput = { Hash64Type.class, SortedListToHash128Type.class, TypesCountType.class,
        PropertiesCountType.class, LongType.class };

    UnitOfWorkTestHelper.addSource(Analytics.getName(GetPropertiesGraph.class), input, typesInput);
    UnitOfWorkTestHelper.addSink(Analytics.getName(GetPropertiesGraph.class), output, typesOutput);
    UnitOfWorkTestHelper.runTestAssembly(GetPropertiesGraph.class, testOutput, properties,
      PropertiesProcessing.SINGLE_TYPE);

    createDictionaries("");

    final List<String> quads = new ArrayList<String>();
    getSummaryQuads(quads, output, RDFMetadataGraph.class, Analytics.getHeadFields(RDFMetadataGraph.class), typesOutput);
    repo = SparqlHelper.createRepositoryFromQuads(quads);

    final String q1 = "?srcNode an:label ?l ." +
                      "?l an:label ?nodeLabel";

    final List<List<Pair>> b1 = new ArrayList<List<Pair>>() {
      {
        add(Lists.newArrayList(
          new Pair("nodeLabel", "http://xmlns.com/foaf/0.1/Person")
        ));
        add(Lists.newArrayList(
          new Pair("nodeLabel", "http://xmlns.com/foaf/0.1/Student")
        ));
      }
    };
    _assertQuery(q1, repo, b1, false);
  }

  /**
   * GL-116
   */
  @Test
  public void testDatatypes()
  throws Exception {
    final Class<?>[] propertiesTypes = { Hash64Type.class, SortedListToHash128Type.class, TypesCountType.class,
        PropertiesCountType.class, LongType.class };

    createDictionaries("testDatatypes");

    final List<String> quads = new ArrayList<String>();
    getSummaryQuads(quads, "src/test/resources/testRDFGraphSummary/testDatatypes/input.check",
      RDFMetadataGraph.class, Analytics.getHeadFields(RDFMetadataGraph.class), propertiesTypes);
    repo = SparqlHelper.createRepositoryFromQuads(quads);

    final String q1 = "?edge an:datatype ?dt ." +
                      "?dt an:label ?label ." +
                      "?dt an:cardinality ?card .";
    final List<List<Pair>> b1 = new ArrayList<List<Pair>>() {
      {
        add(Lists.newArrayList(new Pair("label", "http://acme.org/beer"), new Pair("card", "4")));
        add(Lists.newArrayList(new Pair("label", "http://acme.org/cheese"), new Pair("card", "1")));
      }
    };
    _assertQuery(q1, repo, b1, false);

    final String q2 = "?edge an:label ?label ." +
                      "?edge an:cardinality ?card ." +
                      "?edge an:source ?source .";
    final List<List<Pair>> b2 = new ArrayList<List<Pair>>() {
      {
        add(Lists.newArrayList(new Pair("label", "http://acme.org/produce"), new Pair("card", "5")));
      }
    };
    _assertQuery(q2, repo, b2, false);
  }

  /**
   * GL-116
   */
  @Test
  public void testNoDatatype()
  throws Exception {
    final Class<?>[] propertiesTypes = { Hash64Type.class, SortedListToHash128Type.class, TypesCountType.class,
        PropertiesCountType.class, LongType.class };

    createDictionaries("testNoDatatype");

    final List<String> quads = new ArrayList<String>();
    getSummaryQuads(quads, "src/test/resources/testRDFGraphSummary/testNoDatatype/input.check",
      RDFMetadataGraph.class, Analytics.getHeadFields(RDFMetadataGraph.class), propertiesTypes);
    repo = SparqlHelper.createRepositoryFromQuads(quads);

    final String q1 = "?edge an:datatype ?dt ." +
                      "?dt an:label ?label ." +
                      "?dt an:cardinality ?card .";
    final List<List<Pair>> b1 = new ArrayList<List<Pair>>() {
      {
        add(Lists.newArrayList(new Pair("label", "http://acme.org/beer"), new Pair("card", "4")));
        add(Lists.newArrayList(new Pair("label", "http://acme.org/cheese"), new Pair("card", "1")));
      }
    };
    _assertQuery(q1, repo, b1, false);

    final String q2 = "?edge an:label ?label ." +
                      "?edge an:cardinality ?card ." +
                      "?edge an:source ?source .";
    final List<List<Pair>> b2 = new ArrayList<List<Pair>>() {
      {
        add(Lists.newArrayList(new Pair("label", "http://acme.org/produce"), new Pair("card", "6")));
      }
    };
    _assertQuery(q2, repo, b2, false);
  }

}
