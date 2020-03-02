/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.viz;

import static org.junit.Assert.assertArrayEquals;

import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.junit.Test;
import org.sindice.analytics.benchmark.cascading.viz.BenchmarkResults;
import org.sindice.analytics.benchmark.cascading.viz.ResultsIterator;
import org.sindice.analytics.benchmark.cascading.viz.ResultsProcessor;
import org.sindice.analytics.benchmark.cascading.viz.connectivity.ConnectivityP1Processor;
import org.sindice.analytics.benchmark.cascading.viz.connectivity.ConnectivityP2Processor;
import org.sindice.analytics.benchmark.cascading.viz.connectivity.ConnectivityResultsIterator;
import org.sindice.analytics.benchmark.cascading.viz.machineUsage.MachineUsageProcessor;
import org.sindice.analytics.benchmark.cascading.viz.machineUsage.MachineUsageResultsIterator;
import org.sindice.analytics.benchmark.cascading.viz.schema.SchemaP1Processor;
import org.sindice.analytics.benchmark.cascading.viz.schema.SchemaP2Processor;
import org.sindice.analytics.benchmark.cascading.viz.schema.SchemaResultsIterator;
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

/**
 * 
 */
public class TestResultsProcessor
extends AbstractAnalyticsTestCase {

  @Test
  public void testMachineUsageProcessor()
  throws Exception {
    final Class[] types = new Class[] { StringType.class };
    runResultsProcessors(new MachineUsageResultsIterator(), types, "testMachineUsageProcessor", new MachineUsageProcessor());
  }

  @Test
  public void testMachineUsageProcessor2()
  throws Exception {
    final Class[] types = new Class[] { StringType.class };
    runResultsProcessors(new MachineUsageResultsIterator(), types, "testMachineUsageProcessor2", new MachineUsageProcessor());
  }

  @Test
  public void testSchemaP1Processor()
  throws Exception {
    final Class[] types = new Class[] { Hash64Type.class, SortedListToHash128Type.class, SortedListToHash128Type.class,
        LongType.class, LongType.class, LongType.class, LongType.class };
    runResultsProcessors(new SchemaResultsIterator(), types, "testSchemaP1Processor", new SchemaP1Processor());
  }

  @Test
  public void testSchemaP2Processor()
  throws Exception {
    final Class[] types = new Class[] { Hash64Type.class, SortedListToHash128Type.class, SortedListToHash128Type.class,
        LongType.class, LongType.class, LongType.class, LongType.class };
    runResultsProcessors(new SchemaResultsIterator(), types, "testSchemaP2Processor", new SchemaP2Processor());
  }

  @Test
  public void testConnectivityP1Processor()
  throws Exception {
    final Class[] types = new Class[] { Hash64Type.class, SortedListToHash128Type.class, SortedListToHash128Type.class,
        IntType.class, LongType.class, LongType.class };
    runResultsProcessors(new ConnectivityResultsIterator(), types, "testConnectivityP1Processor", new ConnectivityP1Processor());
  }

  @Test
  public void testConnectivityP2Processor()
  throws Exception {
    final Class[] types = new Class[] { Hash64Type.class, SortedListToHash128Type.class, SortedListToHash128Type.class,
        IntType.class, LongType.class, LongType.class };
    runResultsProcessors(new ConnectivityResultsIterator(), types, "testConnectivityP2Processor", new ConnectivityP2Processor());
  }

  @Test
  public void testSeveralProcessors()
  throws Exception {
    final Class[] types = new Class[] { Hash64Type.class, SortedListToHash128Type.class, SortedListToHash128Type.class,
        IntType.class, LongType.class, LongType.class };
    runResultsProcessors(new ConnectivityResultsIterator(), types, "testSeveralProcessors",
      new ConnectivityP1Processor(), new ConnectivityP2Processor());
  }

  /**
   * Asserts the output of the {@link ResultsProcessor}s. The test folder contains files which name is matched by
   * {@link ResultsIterator#getResultsPattern()}. The expected output of the processor is stored inside
   * the YAML file which name ends with ".ref". Each object of the YAML file corresponds to the
   * output of a single {@link ResultsProcessor}.
   * @param resIt the {@link ResultsIterator} to iterate over the raw data
   * @param types the {@link FieldType}s this {@link ResultsIterator} gives
   * @param folder the name of the test
   * @param dps the set of {@link ResultsProcessor}s to apply on the data
   */
  private void runResultsProcessors(final ResultsIterator resIt,
                                    final Class[] types,
                                    final String folder,
                                    final ResultsProcessor... dps)
  throws Exception {
    final File dir = new File("./src/test/resources/testResultsProcessor/", folder);
    final Fields fields = resIt.getInputFields();
    final String resPattern = resIt.getResultsPattern();
    final File[] files = dir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.matches(resPattern);
      }
    });

    final String dataset = "dog";
    final String algo = "grishka";

    // get the actual results
    final List<BenchmarkResults> actual = new ArrayList<BenchmarkResults>();
    for (int i = 0; i < dps.length; i++) {
      dps[i].reset();
      dps[i].setDataset(dataset);
      dps[i].setAlgorithm(algo);
      for (File file : files) {
        final Tap actualSource = new Hfs(new CheckScheme(fields), file.getAbsolutePath());
        final String actualPath = new File(testOutput, file.getName() + "-actual" + i).getAbsolutePath();
        final Tap actualSink = new Hfs(new SequenceFile(fields), actualPath);
        final Pipe pipe = new Each("test", new ConvertFunction(fields, types));
        final Flow actualFlow = new HadoopFlowConnector(properties).connect(actualSource, actualSink, pipe);
        actualFlow.complete();

        resIt.init(actualFlow.openSink());
        dps[i].process(resIt);
      }
      actual.add(dps[i].getProcessedResults());
    }

    // get the expected results
    final List<BenchmarkResults> expected = new ArrayList<BenchmarkResults>();

    final File[] expectedFiles = dir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".ref");
      }
    });
    for (File file : expectedFiles) {
      final Properties props = new Properties();
      props.load(new FileReader(file));
      final BenchmarkResults br = new BenchmarkResults();
      br.setDataset(dataset);
      br.setAlgorithm(algo);
      for (Entry<Object, Object> entry : props.entrySet()) {
        br.getResults().put(entry.getKey().toString(), entry.getValue().toString());
      }
      expected.add(br);
    }

    Collections.sort(actual);
    Collections.sort(expected);
    assertArrayEquals(expected.toArray(new BenchmarkResults[0]), actual.toArray(new BenchmarkResults[0]));
  }

}
