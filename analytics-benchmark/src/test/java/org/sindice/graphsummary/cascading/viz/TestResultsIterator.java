/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.viz;

import static org.junit.Assert.assertArrayEquals;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.sindice.analytics.benchmark.cascading.viz.ResultsIterator;
import org.sindice.analytics.benchmark.cascading.viz.connectivity.ConnectivityResultsIterator;
import org.sindice.analytics.benchmark.cascading.viz.machineUsage.MachineUsageResultsIterator;
import org.sindice.core.analytics.testHelper.AbstractAnalyticsTestCase;
import org.sindice.core.analytics.testHelper.CheckScheme;
import org.sindice.core.analytics.testHelper.iotransformation.ConvertFunction;
import org.sindice.core.analytics.testHelper.iotransformation.FieldType;
import org.sindice.core.analytics.testHelper.iotransformation.IntType;
import org.sindice.core.analytics.testHelper.iotransformation.LongType;
import org.sindice.core.analytics.testHelper.iotransformation.StringType;
import org.sindice.graphsummary.iotransformation.Hash64Type;
import org.sindice.graphsummary.iotransformation.SortedListToHash128Type;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

/**
 * 
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class TestResultsIterator
extends AbstractAnalyticsTestCase {

  @Test
  public void testConnectivity()
  throws Exception {
    final ResultsIterator resIt = new ConnectivityResultsIterator();
    final Class<? extends FieldType>[] input = new Class[] { Hash64Type.class, SortedListToHash128Type.class,
        SortedListToHash128Type.class, IntType.class, LongType.class, LongType.class };
    final Class<? extends FieldType>[] output = new Class[] { Hash64Type.class, SortedListToHash128Type.class,
        SortedListToHash128Type.class, IntType.class, LongType.class, LongType.class };

    runResultsIterator("testConnectivity", resIt, input, output);
  }

  @Test
  public void testMachineUsage()
  throws Exception {
    final ResultsIterator resIt = new MachineUsageResultsIterator();
    final Class<? extends FieldType>[] input = new Class[] { StringType.class };
    final Class<? extends FieldType>[] output = new Class[] { LongType.class, LongType.class, LongType.class };

    runResultsIterator("testMachineUsage", resIt, input, output);
  }

  /**
   * This method asserts that the raw input data is correctly converted into what is expected.
   * The expected data is in the resource folder of this test, which name is the test method's name and the extension
   * is ".ref".
   * @param test the name of the test method
   * @param resIt the {@link ResultsIterator} to use for iterating over the data
   * @param typesInput the expected {@link FieldType}s of the raw input data
   * @param typesOutput the fields type of the converted data
   */
  private void runResultsIterator(String test,
                                  ResultsIterator resIt,
                                  Class<? extends FieldType>[] typesInput,
                                  Class<? extends FieldType>[] typesOutput)
  throws IOException {
    final File dir = new File("./src/test/resources/testResultsIterator/", test);

    final Fields inputFields = resIt.getInputFields();
    final Fields outputFields = resIt.getOutputFields();
    final String resPattern = resIt.getResultsPattern();

    final File[] files = dir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.matches(resPattern);
      }
    });

    assertTrue(files.length != 0);
    for (File file : files) { // for each file matching the regular expression
      // The input data
      final Tap actualSource = new Hfs(new CheckScheme(inputFields), file.getAbsolutePath());
      final String actualPath = new File(testOutput, file.getName() + "-actual").getAbsolutePath();
      final Tap actualSink = new Hfs(new SequenceFile(inputFields), actualPath);
      final Pipe pipe = new Each("test", new ConvertFunction(inputFields, typesInput));
      final Flow actualFlow = new HadoopFlowConnector(properties).connect(actualSource, actualSink, pipe);
      actualFlow.complete();

      // The expected results from the collected data
      final Tap expectedSource = new Hfs(new CheckScheme(outputFields), file.getAbsolutePath() + ".ref");
      final String expectedPath = new File(testOutput, file.getName() + "-expected").getAbsolutePath();
      final Tap expectedSink = new Hfs(new SequenceFile(outputFields), expectedPath);
      final Pipe expectedPipe = new Each("test", new ConvertFunction(outputFields, typesOutput));
      final Flow expectedFlow = new HadoopFlowConnector(properties).connect(expectedSource, expectedSink, expectedPipe);
      expectedFlow.complete();

      // Iterate over the data
      final List<Tuple> actual = new ArrayList<Tuple>();
      resIt.init(actualFlow.openSink());
      while (resIt.hasNext()) {
        actual.add(resIt.next().getTupleCopy());
      }
      Collections.sort(actual);

      // Assert the outputed data is what is expected
      final List<Tuple> expected = new ArrayList<Tuple>();
      final TupleEntryIterator expectedIt = expectedFlow.openSink();
      while (expectedIt.hasNext()) {
        TupleEntry tupleEntry = expectedIt.next();
        expected.add(tupleEntry.getTupleCopy());
      }
      Collections.sort(expected);
      assertArrayEquals(expected.toArray(new Tuple[0]), actual.toArray(new Tuple[0]));
    }
  }

}
