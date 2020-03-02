/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.relationships;

import org.junit.Before;
import org.junit.Test;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.sindice.core.analytics.testHelper.iotransformation.LongType;
import org.sindice.core.analytics.testHelper.iotransformation.UnitOfWorkTestHelper;
import org.sindice.core.analytics.testHelper.iotransformation.StringType;
import org.sindice.graphsummary.AbstractSummaryTestCase;
import org.sindice.graphsummary.cascading.ecluster.ClusterSubAssembly;
import org.sindice.graphsummary.cascading.entity.GetClusterGraph;
import org.sindice.graphsummary.cascading.entity.GetClusterGraph.PreProcessing;
import org.sindice.graphsummary.iotransformation.EntityDescriptionType;
import org.sindice.graphsummary.iotransformation.Hash128Type;
import org.sindice.graphsummary.iotransformation.Hash64Type;
import org.sindice.graphsummary.iotransformation.SortedListToHash128Type;

import cascading.pipe.Each;
import cascading.pipe.Pipe;

/**
 * @author Thomas Perry <thomas.perry@deri.org> Modified by:
 * @author Pierre Bailly-Ferry <pierre.bailly@deri.org>
 * @author Arthur Baudry <arthur.baudry@deri.org>
 */
public class TestRelationsGraph
extends AbstractSummaryTestCase {

  @Override
  @Before
  public void setUp()
  throws Exception {
    super.setUp();
    properties.setProperty(AnalyticsParameters.CLASS_ATTRIBUTES_FIELD.toString(),
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type,"
        + "http://opengraphprotocol.org/schema/type,"
        + "http://opengraph.org/schema/type,"
        + "http://ogp.me/ns#type,"
        + "http://purl.org/dc/elements/1.1/type,"
        + "http://purl.org/stuff/rev#type,"
        + //Added 19 Oct 2011
        "http://purl.org/dc/terms/type,"
        + //Added 19 Oct 2011
        "http://dbpedia.org/property/type,"
        + "http://dbpedia.org/ontology/type,"
        + "http://dbpedia.org/ontology/Organisation/type,"
        + //Added 25 Oct 2011
        "http://xmlns.com/foaf/0.1/type" //Added 25 Oct 2011
    );
  }

  @Test
  /**
   * Each part of the input file describes one of the cases we have to test when we work with authoritative cluster,
   * a part of the input file is delimited by {}.
   * First part, the entities and their CID are in the same document.
   * Second part, the entities are in the same document and the CID of the second one is in another one, 
   * the relation between them is outgoing.
   * Third part, the entities are in the same document and the CID of the second one is in another one, 
   * the relation between them is incoming.
   * Fourth part, the entities are in the same document and their CIDs are in two different documents which is also different from
   * the document domain of the entities.
   */
  public void testGetClusterTypes()
  throws Exception {
    runRelationsGraph("testGetClusterTypes");
  }

  @Test
  public void testGetClusterTypesOnly()
  throws Exception {
    runRelationsGraph("testGetClusterTypesOnly");
  }

  /**
   * Run the {@link RelationsGraph} assembly
   * @param test the name of the test
   */
  private void runRelationsGraph(String test)
  throws Exception {
    final String inPath = "./src/test/resources/testRelationsGraph/" + test + "/input.check";
    final String inPath1 = "./src/test/resources/testRelationsGraph/" + test + "/input1.check";
    final String outPath = "./src/test/resources/testRelationsGraph/" + test + "/output.check";

    Class<?>[] typesIn = { Hash64Type.class, Hash128Type.class, EntityDescriptionType.class,
        EntityDescriptionType.class };
    Class<?>[] typesIn1 = { Hash64Type.class, Hash128Type.class, EntityDescriptionType.class,
        EntityDescriptionType.class, SortedListToHash128Type.class };
    Class<?>[] typesOut = { Hash64Type.class, Hash64Type.class, SortedListToHash128Type.class, Hash64Type.class,
        Hash64Type.class, SortedListToHash128Type.class, LongType.class };

    UnitOfWorkTestHelper.addSource(Analytics.getName(GetClusterGraph.class), inPath, typesIn);
    UnitOfWorkTestHelper.addSource(Analytics.getName(ClusterSubAssembly.class), inPath1, typesIn1);
    UnitOfWorkTestHelper.addSink(Analytics.getName(RelationsGraph.class), outPath, typesOut);
    UnitOfWorkTestHelper.runTestAssembly(RelationsGraph.class, testOutput, properties);
  }

  @Test
  public void testBlankNode()
  throws Exception {
    final String input = "./src/test/resources/testRelationsGraph/testBlankNode/input.json";
    final String output = "./src/test/resources/testRelationsGraph/testBlankNode/output.check";

    final Class<?>[] typesInputGetClusterGraph = { StringType.class };
    final Class<?>[] typesOutputGetClusterGraph = { Hash64Type.class, Hash128Type.class, Hash64Type.class,
        Hash128Type.class };

    UnitOfWorkTestHelper.addSource(Analytics.getName(TestBlankNode.class), input, typesInputGetClusterGraph);
    UnitOfWorkTestHelper.addSink(Analytics.getName(TestBlankNode.class), output, typesOutputGetClusterGraph);
    UnitOfWorkTestHelper.runTestAssembly(TestBlankNode.class, testOutput, properties);
  }

  @AnalyticsName(value = "test-blanknode")
  @AnalyticsHeadPipes(values = { @AnalyticsPipe(fields = { "value" }) })
  @AnalyticsTailPipes(values = { @AnalyticsPipe(fields = { "domain", "subject-hash", "predicate-hash", "object-hash" }) })
  public static class TestBlankNode
  extends AnalyticsSubAssembly {
    private static final long serialVersionUID = 1L;

    public TestBlankNode(Pipe[] pipes) {
      super(pipes, (Object[]) null);
    }

    @Override
    protected Object assemble(Pipe[] previous, Object... args) {
      Pipe relations = new GetClusterGraph(previous, PreProcessing.O_EDGES_AGGREGATE);
      relations = new Each(relations, new SPO2HashInterclassFunction(Analytics.getTailFields(this)));
      return relations;
    }
  }

}
