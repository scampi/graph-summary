/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.operation;

import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.sindice.core.analytics.cascading.operation.CompactionBy;
import org.sindice.core.analytics.testHelper.AbstractAnalyticsTestCase;
import org.sindice.core.analytics.testHelper.iotransformation.AbstractFieldType;
import org.sindice.core.analytics.testHelper.iotransformation.FieldType;
import org.sindice.core.analytics.testHelper.iotransformation.UnitOfWorkTestHelper;
import org.sindice.core.analytics.testHelper.iotransformation.StringType;
import org.sindice.core.analytics.testHelper.iotransformation.TupleType;

import cascading.flow.FlowProcess;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.AggregateBy;
import cascading.tuple.Fields;

public class TestCompactionBy
extends AbstractAnalyticsTestCase {

  @AnalyticsName(value = "test-compact")
  @AnalyticsHeadPipes(values = { @AnalyticsPipe(fields = { "group", "data" }) })
  @AnalyticsTailPipes(values = { @AnalyticsPipe(fields = { "group", "data" }) })
  public static class Compact extends AnalyticsSubAssembly {
    public Compact(Pipe[] pipes) {
      super(pipes, (Object[]) null);
    }
    @Override
    protected Object assemble(Pipe[] previous, Object... args) {
      return new AggregateBy(previous[0], new Fields("group"), new CompactionBy(new Fields("data")));
    }
  }

  @Test
  public void testCompactStrings()
  throws Exception {
    runCompaction("testCompact");
  }

  public static class TextType
  extends AbstractFieldType<Text> {

    public TextType(FlowProcess fp, String input) {
      super(fp, input);
    }

    @Override
    protected Text doConvert() {
      return new Text(input);
    }

    @Override
    protected Text getEmptyField() {
      return new Text();
    }

  }

  @Test
  public void testCompactTexts()
  throws Exception {
    properties.setProperty(TupleType.FIELD_TYPE, TextType.class.getName());
    runCompaction("testCompact", TextType.class);
  }

  @Test
  public void testCompactTuples()
  throws Exception {
    runCompaction("testCompact", TupleType.class);
  }

  @Test
  public void testNull()
  throws Exception {
    runCompaction("testNull");
  }

  private void runCompaction(String folder)
  throws Exception {
    runCompaction(folder, null);
  }

  /**
   * Runs a {@link SubAssembly} for testing the {@link CompactionBy}.
   * @param folder the name of the test
   * @param type the {@link FieldType} of the input data field
   */
  private void runCompaction(String folder, Class<? extends FieldType> type)
  throws Exception {
    final String input = "./src/test/resources/testCompactionBy/" + folder + "/input.check";
    final String output = "./src/test/resources/testCompactionBy/" + folder + "/output.check";

    Class<?>[] typesInput = { StringType.class, type == null ? StringType.class : type };
    Class<?>[] typesOutput = { StringType.class, TupleType.class };

    UnitOfWorkTestHelper.addSource(Analytics.getName(Compact.class), input, typesInput);
    UnitOfWorkTestHelper.addSink(Analytics.getName(Compact.class), output, typesOutput);
    UnitOfWorkTestHelper.runTestAssembly(Compact.class, testOutput, properties);
  }

}
