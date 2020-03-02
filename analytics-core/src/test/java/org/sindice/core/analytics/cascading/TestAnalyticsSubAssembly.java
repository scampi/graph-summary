/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.sindice.core.analytics.testHelper.AbstractAnalyticsTestCase;
import org.sindice.core.analytics.testHelper.iotransformation.UnitOfWorkTestHelper;
import org.sindice.core.analytics.testHelper.iotransformation.IntType;
import org.sindice.core.analytics.testHelper.iotransformation.StringType;

import cascading.operation.Insert;
import cascading.operation.state.Counter;
import cascading.pipe.Each;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

public class TestAnalyticsSubAssembly
extends AbstractAnalyticsTestCase {

  @Test
  public void testAssertCounters()
  throws Exception {
    final String input = "./src/test/resources/testAnalyticsSubAssembly/testAssertCounters/input.txt";
    final Map<String, Map<String, Long>> counters = new HashMap<String, Map<String,Long>>();

    counters.put("g", new HashMap<String, Long>());
    counters.get("g").put("c", 4L);

    Class<?>[] types = { StringType.class };

    UnitOfWorkTestHelper.addSource("testAssertCounters", input, types);
    UnitOfWorkTestHelper.addSink("testAssertCounters", input, types);
    UnitOfWorkTestHelper.runTestAssembly(TestAssertCounters.class, testOutput, properties, counters);
  }

  @AnalyticsName(value = "testAssertCounters")
  @AnalyticsHeadPipes(values = { @AnalyticsPipe(fields = { "value" }) })
  @AnalyticsTailPipes(values = { @AnalyticsPipe(fields = { "value" }) })
  public static class TestAssertCounters extends AnalyticsSubAssembly {
    private static final long serialVersionUID = 1L;

    public TestAssertCounters(Pipe[] pipes) {
      super(pipes, (Object[]) null);
    }

    @Override
    protected Object assemble(Pipe[] previous, Object... args) {
      return new Each(previous[0], new Counter("g", "c"));
    }

  }

  @Test
  public void testArguments()
  throws Exception {
    final String inputPathSource = "./src/test/resources/testAnalyticsSubAssembly/"
                                   + "testArguments/input.txt";
    final String outputPathSource = "./src/test/resources/testAnalyticsSubAssembly/"
                                   + "testArguments/output.txt";

    Class<?>[] typesInput = { StringType.class };
    Class<?>[] typesOutput = { StringType.class, StringType.class, IntType.class };

    UnitOfWorkTestHelper.addSource("test", inputPathSource, typesInput);
    UnitOfWorkTestHelper.addSink("test", outputPathSource, typesOutput);
    UnitOfWorkTestHelper.runTestAssembly(TestArguments.class,
      testOutput, properties, "stephane", 30);
  }

  @AnalyticsName(value = "test")
  @AnalyticsHeadPipes(values = { @AnalyticsPipe(fields = { "value" }) })
  @AnalyticsTailPipes(values = { @AnalyticsPipe(fields = { "value", "name", "number" }) })
  public static class TestArguments extends AnalyticsSubAssembly {
    private static final long serialVersionUID = 1L;

    public TestArguments(Pipe[] pipes, Object...args) {
      super(pipes, args);
    }

    @Override
    protected Object assemble(Pipe[] previous, Object... args) {
      Pipe pipe = new Each(previous[0], new Fields("value"),
        new Insert(new Fields("name", "number"), args),
        Analytics.getTailFields(this));
      return pipe;
    }

  }

  @Test
  public void testMultiInputPipes()
  throws Exception {
    final String i1 = "./src/test/resources/testAnalyticsSubAssembly/testMultiInputPipes/input1.txt";
    final String o1 = "./src/test/resources/testAnalyticsSubAssembly/testMultiInputPipes/output1.txt";
    final String o2 = "./src/test/resources/testAnalyticsSubAssembly/testMultiInputPipes/output2.txt";
    final String o = "./src/test/resources/testAnalyticsSubAssembly/testMultiInputPipes/output.txt";

    Class<?>[] typesI1 = { StringType.class };
    Class<?>[] typesO12 = { StringType.class, StringType.class };
    Class<?>[] typesOutput = { StringType.class, StringType.class, StringType.class };

    UnitOfWorkTestHelper.addSource(Analytics.getName(TestMultiInputPipes1.class), i1, typesI1);
    UnitOfWorkTestHelper.addSink("sink1", o1, typesO12);
    UnitOfWorkTestHelper.addSink("sink2", o2, typesO12);
    UnitOfWorkTestHelper.runTestAssembly(TestMultiInputPipes1.class, testOutput, properties);

    UnitOfWorkTestHelper.reset();
    UnitOfWorkTestHelper.addSource("head1", o1, typesO12);
    UnitOfWorkTestHelper.addSource("head2", o2, typesO12);
    UnitOfWorkTestHelper.addSink(Analytics.getName(TestMultiInputPipes2.class), o, typesOutput);
    UnitOfWorkTestHelper.runTestAssembly(TestMultiInputPipes2.class, testOutput, properties);
  }

  @AnalyticsName(value = "testMultiInputPipes-1")
  @AnalyticsHeadPipes(values = { @AnalyticsPipe(fields = { "value" }) })
  @AnalyticsTailPipes(values = {
    @AnalyticsPipe(name="sink1", fields = { "name", "value" }),
    @AnalyticsPipe(name="sink2", fields = { "name", "value" })
  })
  public static class TestMultiInputPipes1 extends AnalyticsSubAssembly {
    private static final long serialVersionUID = 1L;

    public TestMultiInputPipes1(Pipe[] pipes) {
      super(pipes, (Object[]) null);
    }

    @Override
    protected Object assemble(Pipe[] previous, Object... args) {
      Pipe pipe1 = new Each(new Pipe("sink1", previous[0]), new Fields("value"),
        new Insert(new Fields("name"), "This is sink1"), new Fields("name", "value"));
      Pipe pipe2 = new Each(new Pipe("sink2", previous[0]), new Fields("value"),
        new Insert(new Fields("name"), "This is sink2"), new Fields("name", "value"));
      return Pipe.pipes(pipe1, pipe2);
    }

  }

  @AnalyticsName(value = "testMultiInputPipes-2")
  @AnalyticsHeadPipes(values = {
    @AnalyticsPipe(name="head1", from=TestMultiInputPipes1.class, sinkName="sink1"),
    @AnalyticsPipe(name="head2", from=TestMultiInputPipes1.class, sinkName="sink2")
  })
  @AnalyticsTailPipes(values = { @AnalyticsPipe(fields = { "a", "name", "value" }) })
  public static class TestMultiInputPipes2 extends AnalyticsSubAssembly {
    private static final long serialVersionUID = 1L;

    public TestMultiInputPipes2(Pipe[] pipes) {
      super(pipes, (Object[]) null);
    }

    @Override
    protected Object assemble(Pipe[] previous, Object... args) {
      Pipe pipe1 = new Each(previous[0], new Fields("value"),
        new Insert(new Fields("a"), "This is head1"), Analytics.getTailFields(TestMultiInputPipes2.class));
      Pipe pipe2 = new Each(previous[1], new Fields("value"),
        new Insert(new Fields("a"), "This is head2"), Analytics.getTailFields(TestMultiInputPipes2.class));
      Pipe pipe = new Merge(Analytics.getName(this), Pipe.pipes(pipe1, pipe2));
      return pipe;
    }

  }

}
