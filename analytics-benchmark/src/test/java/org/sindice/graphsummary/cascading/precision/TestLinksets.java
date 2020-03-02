/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.precision;

import org.junit.Test;
import org.sindice.analytics.benchmark.cascading.precision.Linksets;
import org.sindice.core.analytics.testHelper.iotransformation.LongType;
import org.sindice.core.analytics.testHelper.iotransformation.UnitOfWorkTestHelper;
import org.sindice.core.analytics.testHelper.iotransformation.TupleType;
import org.sindice.graphsummary.AbstractSummaryTestCase;
import org.sindice.graphsummary.iotransformation.EntityDescriptionType;
import org.sindice.graphsummary.iotransformation.Hash128Type;
import org.sindice.graphsummary.iotransformation.Hash64Type;
import org.sindice.graphsummary.iotransformation.PropertiesCountType;
import org.sindice.graphsummary.iotransformation.SortedListToHash128Type;
import org.sindice.graphsummary.iotransformation.TypesCountType;

public class TestLinksets extends AbstractSummaryTestCase {

  @Test
  public void testLinksets()
  throws Exception {
    runLinksets("testLinksets");
  }

  /**
   * Test for the link accuracy when a gold cluster has no outgoing relation,
   * but does through its ~-equivalence class.
   */
  @Test
  public void testLinkInSummaryOnly()
  throws Exception {
    runLinksets("testLinkInSummaryOnly");
  }

  /**
   * Test for the blank equivalence class
   */
  @Test
  public void testBlankEquivalenceClass()
  throws Exception {
    runLinksets("testBlankEquivalenceClass");
  }

  /**
   * Test for the blank equivalence class
   */
  @Test
  public void testBlankInMiddle()
  throws Exception {
    runLinksets("testBlankInMiddle");
  }

  @Test
  public void testTypes()
  throws Exception {
    runLinksets("testTypes");
  }

  /**
   * This method runs the {@link Linksets} assembly on the data from the given folder.
   * The folder contains a file for each head and tail of the {@link Linksets}.
   */
  private void runLinksets(final String folder)
  throws Exception {
    final String relationsGoldPath = RES + "testPrecision/testLinksets/" + folder + "/gold-relations.check";
    final String propertiesGoldPath = RES + "testPrecision/testLinksets/" + folder + "/gold-properties.check";
    final String entityTableGoldPath = RES + "testPrecision/testLinksets/" + folder + "/gold-entity-table.check";

    final String relationsEvalPath = RES + "testPrecision/testLinksets/" + folder + "/eval-relations.check";
    final String propertiesEvalPath = RES + "testPrecision/testLinksets/" + folder + "/eval-properties.check";
    final String entityTableEvalPath = RES + "testPrecision/testLinksets/" + folder + "/eval-entity-table.check";

    final String evalLinksetPath = RES + "testPrecision/testLinksets/" + folder + "/eval-linkset.check";
    final String geTablePath = RES + "testPrecision/testLinksets/" + folder + "/gold-eval-table.check";

    Class<?>[] relationsTypes = { Hash64Type.class, Hash64Type.class, SortedListToHash128Type.class, Hash64Type.class,
        Hash64Type.class, SortedListToHash128Type.class, LongType.class };
    Class<?>[] propsTypes = { Hash64Type.class, SortedListToHash128Type.class, TypesCountType.class,
        PropertiesCountType.class, LongType.class };
    Class<?>[] eTableTypes = { Hash64Type.class, Hash128Type.class, EntityDescriptionType.class,
        EntityDescriptionType.class, SortedListToHash128Type.class };

    properties.setProperty(TupleType.FIELD_TYPE, Hash64Type.class.getName());
    Class<?>[] evalLinksetTypes = { Hash64Type.class, SortedListToHash128Type.class, LongType.class,
      SortedListToHash128Type.class, LongType.class, TupleType.class,
      SortedListToHash128Type.class, LongType.class, SortedListToHash128Type.class, LongType.class, TupleType.class };
    Class<?>[] geTableTypes = { Hash64Type.class, SortedListToHash128Type.class, SortedListToHash128Type.class,
      LongType.class, TypesCountType.class, PropertiesCountType.class,
      TypesCountType.class, PropertiesCountType.class };

    UnitOfWorkTestHelper.addSource("gold-relations", relationsGoldPath, relationsTypes);
    UnitOfWorkTestHelper.addSource("gold-properties", propertiesGoldPath, propsTypes);
    UnitOfWorkTestHelper.addSource("gold-entity-table", entityTableGoldPath, eTableTypes);
    UnitOfWorkTestHelper.addSource("eval-relations", relationsEvalPath, relationsTypes);
    UnitOfWorkTestHelper.addSource("eval-properties", propertiesEvalPath, propsTypes);
    UnitOfWorkTestHelper.addSource("eval-entity-table", entityTableEvalPath, eTableTypes);
    UnitOfWorkTestHelper.addSink("eval-linkset", evalLinksetPath, evalLinksetTypes);
    UnitOfWorkTestHelper.addSink("gold-eval-table", geTablePath, geTableTypes);
    UnitOfWorkTestHelper.runTestAssembly(Linksets.class, testOutput, properties);
  }

}
