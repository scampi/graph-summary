/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.precision;

import org.junit.Test;
import org.sindice.analytics.benchmark.cascading.precision.schema.SchemaAssembly;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.testHelper.iotransformation.LongType;
import org.sindice.core.analytics.testHelper.iotransformation.UnitOfWorkTestHelper;
import org.sindice.graphsummary.AbstractSummaryTestCase;
import org.sindice.graphsummary.iotransformation.Hash64Type;
import org.sindice.graphsummary.iotransformation.PropertiesCountType;
import org.sindice.graphsummary.iotransformation.SortedListToHash128Type;
import org.sindice.graphsummary.iotransformation.TypesCountType;

public class TestSchemaAssembly extends AbstractSummaryTestCase {

  @Test
  public void testSchema1()
  throws Exception {
    runSchemaAssembly("testSchema1");
  }

  @Test
  public void testSchema2()
  throws Exception {
    runSchemaAssembly("testSchema2");
  }

  @Test
  public void testSchemaBlank()
  throws Exception {
    runSchemaAssembly("testSchemaBlank");
  }

  private void runSchemaAssembly(final String folder)
  throws Exception {
    final String geTablePath = RES + "testPrecision/testSchemaAssembly/" + folder + "/gold-eval-table.check";
    final String schemaPath = RES + "testPrecision/testSchemaAssembly/" + folder + "/schema.check";

    Class<?>[] schemaTypes = { Hash64Type.class, SortedListToHash128Type.class, SortedListToHash128Type.class,
        LongType.class, LongType.class, LongType.class, LongType.class };
    Class<?>[] geTableTypes = { Hash64Type.class, SortedListToHash128Type.class, SortedListToHash128Type.class,
        LongType.class, TypesCountType.class, PropertiesCountType.class,
        TypesCountType.class, PropertiesCountType.class };

    UnitOfWorkTestHelper.addSource(Analytics.getName(SchemaAssembly.class), geTablePath, geTableTypes);
    UnitOfWorkTestHelper.addSink(Analytics.getName(SchemaAssembly.class), schemaPath, schemaTypes);
    UnitOfWorkTestHelper.runTestAssembly(SchemaAssembly.class, testOutput, properties);
  }

}
