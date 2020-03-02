/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.entity;

import org.junit.Test;
import org.sindice.analytics.entity.EntityDescriptionFactory;
import org.sindice.analytics.entity.EntityDescriptionFactory.Type;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.testHelper.iotransformation.StringType;
import org.sindice.core.analytics.testHelper.iotransformation.UnitOfWorkTestHelper;
import org.sindice.graphsummary.AbstractSummaryTestCase;
import org.sindice.graphsummary.cascading.entity.GetClusterGraph.PreProcessing;
import org.sindice.graphsummary.iotransformation.EntityDescriptionType;
import org.sindice.graphsummary.iotransformation.Hash128Type;
import org.sindice.graphsummary.iotransformation.Hash64Type;

/**
 * @author Pierre Bailly-Ferry <pierre.bailly@deri.org> Modified by :
 * @author Arthur Baudry <arthur.baudry@deri.org>
 */
public class TestGetClusterGraph
extends AbstractSummaryTestCase {

  @Test
  public void testGetCIDTypes()
  throws Exception {
    run("testGetCIDTypes");
  }

  @Test
  public void testPropertiesGraph()
  throws Exception {
    run("testPropertiesGraph");
  }

  @Test
  public void testDuplicateTypes()
  throws Exception {
    run("testDuplicateTypes");
  }

  @Test
  public void testExcludeProperty()
  throws Exception {
    properties.setProperty(AnalyticsParameters.PREDICATES_BLACKLIST.toString(), "http://www.predicate.com/#p1");
    run("testExcludeProperty");
  }

  @Test
  public void testExcludePropertyRegexp()
  throws Exception {
    properties.setProperty(AnalyticsParameters.PREDICATES_BLACKLIST_REGEXP.toString(), "http://www.predicate.com/#p\\d+");
    run("testExcludePropertyRegexp");
  }

  @Test
  public void testGetCIDPropertiesEmptyTypes()
  throws Exception {
    run("testGetCIDPropertiesEmptyTypes");
  }

  @Test
  public void testGetCIDPropertiesEmptyProperties()
  throws Exception {
    run("testGetCIDPropertiesEmptyProperties");
  }

  @Test
  public void testGetCIDPropertiesTypes()
  throws Exception {
    run("testGetCIDPropertiesTypes");
  }

  @Test
  public void testGetCIDSingleType()
  throws Exception {
    run("testGetCIDSingleType");
  }

  @Test
  public void testBlankNode()
  throws Exception {
    run("testBlankNode");
  }

  /**
   * GL-24
   * <p>
   * In GL-40, we changed the algorithm of {@link SubjectAggregateBy}, where there is no
   * need to parse the statements (a second time). The issue reported in https://openrdf.atlassian.net/browse/SES-1651
   * can still be seen only with {@link StringEntityDescription}, when iterating over the statements using
   * {@link StringEntityDescription#iterateStatements()}.
   * <p>
   * With GL-103, there is no ever again to parse the statements.
   */
  @Test
  public void testBadEntityURI()
  throws Exception {
    properties.setProperty(EntityDescriptionFactory.ENTITY_DESCRIPTION, Type.MAP.toString());
    run("testBadEntityURI");
  }

  /**
   * Run the {@link GetClusterGraph}
   */
  private void run(String folder)
  throws Exception {
    final String input = "./src/test/resources/testGetClusterGraph/" + folder + "/input.check";
    final String output = "./src/test/resources/testGetClusterGraph/" + folder + "/output.check";

    Class<?>[] typesInput = { StringType.class };
    Class<?>[] typesOutput = { Hash64Type.class, Hash128Type.class,
        EntityDescriptionType.class, EntityDescriptionType.class };

    UnitOfWorkTestHelper.addSource(Analytics.getName(GetClusterGraph.class), input, typesInput);
    UnitOfWorkTestHelper.addSink(Analytics.getName(GetClusterGraph.class), output, typesOutput);
    UnitOfWorkTestHelper.runTestAssembly(GetClusterGraph.class, testOutput, properties, PreProcessing.O_EDGES_AGGREGATE);
  }

}
