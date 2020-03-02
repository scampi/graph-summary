/*******************************************************************************
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 *
 *
 * This project is a free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This project is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this project. If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.sindice.core.analytics.stats.basic.cli;

import java.io.IOException;
import java.util.Properties;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;
import org.sindice.core.analytics.cascading.CascadeConfYAMLoader;
import org.sindice.core.analytics.stats.basic.assembly.BasicFormatStats;
import org.sindice.core.analytics.util.AnalyticsException;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.management.UnitOfWork;
import cascading.property.AppProps;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.WritableSequenceFile;
import cascading.stats.CascadingStats;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

/**
 * @author Diego Ceccarelli <diego.ceccarelli@deri.org>
 * @link www.di.unipi.it/~ceccarel
 */
public class BasicFormatStatsCLI
extends AbstractAnalyticsCLI {

  /**
   * Executes the basic class statistics Cascading job.
   * 
   * @param options
   *          The options for the job. See options in
   *          {@link #initializeOptionParser(OptionParser)
   *          initializeOptionParser}
   * @throws AnalyticsException
   *           AnalyticsExceptoin
   * @throws IOException
   *           IOException
   */
  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(final OptionSet options)
  throws Exception {
    final Properties properties = cascadeConf.getFlowsConfiguration()
    .get(CascadeConfYAMLoader.DEFAULT_PARAMETERS);

    AppProps.setApplicationJarClass(properties, BasicFormatStatsCLI.class);

    // Check for input directory
    final Fields fieldSelector = new Fields("value");
    final Tap<?, ?, ?> source = new Hfs(new WritableSequenceFile(fieldSelector, Text.class), input.get(0));

    // Set source, sink and fields for job
    final BasicFormatStats pipe = new BasicFormatStats();
    final Fields fd = new Fields("format", "url", "domain", "sndDomain");
    final Flow<?> flow = new HadoopFlowConnector(properties)
    .connect("basic-format-stats", source, new Hfs(new SequenceFile(fd), output.get(0), SinkMode.REPLACE), pipe);

    // Run the job
    flow.start();
    flow.complete();

    return flow;
  }

  public static void main(final String[] args)
  throws Exception {
    final BasicFormatStatsCLI cli = new BasicFormatStatsCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
