/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.testHelper;

import java.io.File;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.sindice.analytics.entity.AnalyticsValueSerialization;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
import org.sindice.core.analytics.rdf.AnalyticsClassAttributes;
import org.sindice.core.analytics.testHelper.iotransformation.UnitOfWorkTestHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.tuple.hadoop.BytesSerialization;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.hadoop.TupleSerializationProps;

/**
 * Base class for writing tests in the Analytics.
 * <p>
 * The {@link Rule} {@link Failure} allows to print the current set of parameters when a test fails.
 */
@RunWith(BlockJUnit4ClassRunner.class)
public abstract class AbstractAnalyticsTestCase extends TestCase {

  private static final Logger   logger     = LoggerFactory.getLogger(AbstractAnalyticsTestCase.class);

  protected File                testOutput;
  protected final Properties    properties = new Properties();
  /** The path to the resources test folder */
  protected final static String RES        = "./src/test/resources/";

  @Rule
  public final Failure fail = new Failure();

  @BeforeClass
  public static void setUpBeforeClass() {
    AnalyticsParameters.reset();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    UnitOfWorkTestHelper.reset();
    AnalyticsParameters.reset();
    properties.clear();
    // Add the Cascading + Hadoop serializations
    TupleSerializationProps.addSerialization(properties, TupleSerialization.class.getName());
    TupleSerializationProps.addSerialization(properties, WritableSerialization.class.getName());
    TupleSerializationProps.addSerialization(properties, BytesSerialization.class.getName());
    // Use the custom Analytics serializations
    TupleSerializationProps.addSerialization(properties, AnalyticsValueSerialization.class.getName());

    testOutput = new File(System.getProperty("java.io.tmpdir"), "testOutput" + Math.random());
    testOutput.mkdirs();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    if (testOutput != null) {
      FileUtil.fullyDelete(testOutput);
    }
    AnalyticsClassAttributes.CLASS_ATTRIBUTES = null;
  }

  /**
   * This {@link TestWatcher} prints the parameters used in the failed run.
   */
  public class Failure extends TestWatcher {

    @Override
    protected void failed(Throwable e, Description description) {
      super.failed(e, description);
      logger.error("Error in {}. # Analytics Parameters: {}", description.toString(), properties);
    }

  }

}
