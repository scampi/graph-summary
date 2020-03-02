/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.viz;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;
import org.sindice.analytics.benchmark.cascading.viz.Exporter;
import org.sindice.analytics.benchmark.cascading.viz.Formatter.FormatterType;
import org.sindice.analytics.benchmark.cascading.viz.ResultsExporter;
import org.sindice.analytics.benchmark.cascading.viz.connectivity.ConnectivityExporter;
import org.sindice.analytics.benchmark.cascading.viz.machineUsage.MachineUsageExporter;
import org.sindice.analytics.benchmark.cascading.viz.schema.SchemaExporter;
import org.sindice.analytics.benchmark.cascading.viz.volume.VolumeExporter;
import org.sindice.core.analytics.cascading.scheme.WholeFile;
import org.sindice.core.analytics.testHelper.AbstractAnalyticsTestCase;
import org.sindice.core.analytics.testHelper.CheckScheme;
import org.sindice.core.analytics.testHelper.iotransformation.UnitOfWorkTestHelper;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.scheme.Scheme;

/**
 * 
 */
public class TestResultsExporter
extends AbstractAnalyticsTestCase {

  /**
   * GL-82
   */
  @Test
  public void testMissingValues()
  throws Exception {
    runResultsExporter("testMissingValues", FormatterType.LATEX, new ConnectivityExporter());
  }

  /**
   * GL-90
   */
  @Test
  public void testMissingFoldersValues()
  throws Exception {
    runResultsExporter("testMissingFoldersValues", FormatterType.LATEX, new ConnectivityExporter());
  }

  @Test
  public void testVolumeExporter()
  throws Exception {
    runResultsExporter("testVolumeExporter", FormatterType.LATEX, new VolumeExporter());
  }

  @Test
  public void testConnectivityExporter()
  throws Exception {
    runResultsExporter("testConnectivityExporter", FormatterType.LATEX, new ConnectivityExporter());
  }

  @Test
  public void testSchemaExporter()
  throws Exception {
    runResultsExporter("testSchemaExporter", FormatterType.LATEX, new SchemaExporter());
  }

  @Test
  public void testMachineUsageExporter()
  throws Exception {
    runResultsExporter("testMachineUsageExporter", FormatterType.LATEX, WholeFile.class, new MachineUsageExporter());
  }

  private void runResultsExporter(final String folder, final FormatterType ft, final ResultsExporter... exporters)
  throws Exception {
    runResultsExporter(folder, ft, CheckScheme.class, exporters);
  }

  /**
   * Run the {@link ResultsExporter}s over the given folder, using the given {@link FormatterType}.
   * A file which name is {@link ResultsExporter#getName()} with the extension ".ref" is located at the root of the
   * given folder. This file is used to assert the generated export.
   * @param folder the name of the folder
   * @param ft the {@link FormatterType}
   * @param exporters the list of {@link ResultsExporter} to apply
   */
  private void runResultsExporter(final String folder, final FormatterType ft, final Class<? extends Scheme> scheme, final ResultsExporter... exporters)
  throws Exception {
    final File dir = new File("./src/test/resources/testResultsExporter/" + folder);
    final Exporter exp = new Exporter();
    final FlowProcess fp = new HadoopFlowProcess(HadoopUtil.createJobConf(properties, new JobConf()));

    for (ResultsExporter res : exporters) {
      res.setSchemeClass(scheme);
    }
    exp.export(ft, dir, testOutput, fp, exporters);

    for (ResultsExporter res : exporters) {
      final BufferedReader expected = new BufferedReader(new FileReader(new File(dir, res.getName() + ".ref")));
      final BufferedReader actual = new BufferedReader(new FileReader(new File(testOutput, res.getName())));
      UnitOfWorkTestHelper.assertReadersEquals(expected, actual);
    }
  }

}
