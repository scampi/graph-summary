/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.sindice.analytics.entity.EntityDescriptionFactory;
import org.sindice.analytics.entity.EntityDescriptionFactory.Type;
import org.sindice.analytics.entity.EntityDescriptionSerialization;
import org.sindice.core.analytics.testHelper.AbstractAnalyticsTestCase;
import org.sindice.graphsummary.cascading.SummaryParameters;
import org.sindice.graphsummary.cascading.properties.PropertiesCountSerialization;
import org.sindice.graphsummary.cascading.properties.TypesCountSerialization;

import cascading.tuple.hadoop.TupleSerializationProps;

/**
 * Base class for writing tests in the summary.
 * This class sets summary specific configuration parameters.
 */
public abstract class AbstractSummaryTestCase
extends AbstractAnalyticsTestCase {

  @BeforeClass
  public static void setUpBeforeClass() {
    SummaryParameters.reset();
  }

  @Override
  @Before
  public void setUp()
  throws Exception {
    super.setUp();
    SummaryParameters.reset();
    // Randomly choose an EntityDescription implementation
    final String[] desc = {
      Type.MAP.toString()
    };
    properties.setProperty(EntityDescriptionFactory.ENTITY_DESCRIPTION, desc[RandomUtils.nextInt(desc.length)]);
    // Adds serializations
    TupleSerializationProps.addSerialization(properties, EntityDescriptionSerialization.class.getName());
    TupleSerializationProps.addSerialization(properties, PropertiesCountSerialization.class.getName());
    TupleSerializationProps.addSerialization(properties, TypesCountSerialization.class.getName());
  }

}
