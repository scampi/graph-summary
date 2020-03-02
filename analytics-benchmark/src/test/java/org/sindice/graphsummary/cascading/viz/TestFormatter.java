/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.viz;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;
import org.sindice.analytics.benchmark.cascading.viz.BenchmarkResults;
import org.sindice.analytics.benchmark.cascading.viz.Formatter;
import org.sindice.analytics.benchmark.cascading.viz.Formatter.FormatterType;
import org.sindice.analytics.benchmark.cascading.viz.ResultsProcessor;
import org.sindice.analytics.benchmark.cascading.viz.connectivity.ConnectivityHtmlFormatter;
import org.sindice.analytics.benchmark.cascading.viz.connectivity.ConnectivityLatexFormatter;
import org.sindice.analytics.benchmark.cascading.viz.machineUsage.MachineUsageLatexFormatter;
import org.sindice.analytics.benchmark.cascading.viz.schema.SchemaLatexFormatter;
import org.sindice.core.analytics.testHelper.AbstractAnalyticsTestCase;
import org.sindice.core.analytics.testHelper.iotransformation.UnitOfWorkTestHelper;

/**
 * 
 */
public class TestFormatter
extends AbstractAnalyticsTestCase {

  /**
   * GL-81
   */
  @Test
  public void testEscapeLatex()
  throws Exception {
    runFormatter(new ConnectivityLatexFormatter(), "testEscapeLatex", "cat#dog", "grishka$wajka");
  }

  @Test
  public void testLatex1()
  throws Exception {
    runFormatter(new ConnectivityLatexFormatter(), "testLatex1");
  }

  @Test
  public void testLatex2()
  throws Exception {
    runFormatter(new ConnectivityLatexFormatter(), "testLatex2");
  }

  @Test
  public void testConnectivityHtml()
  throws Exception {
    runFormatter(new ConnectivityHtmlFormatter(), "testConnectivityHtml");
  }

  @Test
  public void testSchemaLatex()
  throws Exception {
    runFormatter(new SchemaLatexFormatter(), "testSchemaLatex");
  }

  @Test
  public void testMachineUsageLatex()
  throws Exception {
    runFormatter(new MachineUsageLatexFormatter(), "testMachineUsageLatex");
  }

  /**
   * Exports the data using the given {@link FormatterType}.
   * The input data, which is expected to come from several {@link ResultsProcessor}s, is contained in multiple files.
   * The name of the files follow the regular expression {@code input[0-9]*}.
   * The formatted data is asserted against the file named "ref".
   * <p>
   * The dataset name is set to <b>dog</b> and the algorithm name is set to <b>grishka</b>.
   * @param f the {@link FormatterType} to test
   * @param folder the name of the test folder
   * @throws Exception
   */
  private void runFormatter(final Formatter f, final String folder)
  throws Exception {
    runFormatter(f, folder, "dog", "grishka");
  }

  /**
   * Exports the data using the given {@link FormatterType}.
   * The input data, which is expected to come from several {@link ResultsProcessor}s, is contained in multiple files.
   * The name of the files follow the regular expression {@code input[0-9]*}.
   * The formatted data is asserted against the file named "ref".
   * @param f the {@link FormatterType} to test
   * @param folder the name of the test folder
   * @param dataset the dataset name
   * @param algorithm the algorithm name
   * @throws Exception
   */
  private void runFormatter(final Formatter f, final String folder, final String dataset, final String algorithm)
  throws Exception {
    final File dir = new File("./src/test/resources/testFormatter/", folder);

    // the actual formatted data
    final File actual = new File(testOutput, folder);
    final FileWriter out = new FileWriter(actual);

    try {
      final List<BenchmarkResults> data = new ArrayList<BenchmarkResults>();
      final File[] files = dir.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.matches("input[0-9]*");
        }
      });

      for (File file : files) {
        final Properties map = new Properties();
        map.load(new FileReader(file));
        final BenchmarkResults br = new BenchmarkResults();
        br.setDataset(dataset);
        br.setAlgorithm(algorithm);
        for (Entry<Object, Object> entry : map.entrySet()) {
          br.getResults().put(entry.getKey().toString(), entry.getValue().toString());
        }
        data.add(br);
      }

      f.start(out);
      // Set the algorithms
      final Set<String> algosName = new TreeSet<String>();
      algosName.add(algorithm);
      f.setAlgorithms(out, algosName);
      f.addBenchmarkResults(out, data);
      f.end(out);
    } finally {
      out.close();
    }
    // The expected output
    final BufferedReader rExpected = new BufferedReader(new FileReader(new File(dir, "ref")));
    final BufferedReader rActual = new BufferedReader(new FileReader(actual));
    UnitOfWorkTestHelper.assertReadersEquals(rExpected, rActual);
  }

}
