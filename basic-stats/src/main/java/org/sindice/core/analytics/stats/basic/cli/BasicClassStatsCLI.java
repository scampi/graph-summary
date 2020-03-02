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
import java.util.List;
import java.util.Properties;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
import org.sindice.core.analytics.cascading.CascadeConfYAMLoader;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.stats.basic.assembly.BasicClassStats;
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

/**
 * @project analytics
 * @author Diego Ceccarelli <diego.ceccarelli@deri.org>
 * @link www.di.unipi.it/~ceccarel
 * @copyright Copyright (C) 2011, All rights reserved.
 */
public class BasicClassStatsCLI extends AbstractAnalyticsCLI {

  private static final String CLASS_ATTRIBUTES = "class-attributes";

  /**
   * Initialise parser options with options for input directory, output
   * directory, and consider only rdf or rdfa.
   * 
   * @param parser
   *          The OptionParser, see valid options below.
   * @param -input The input directory
   * @param -output The output directory
   * @param -rdf Consider only rdf or rdfa
   */
  @Override
  protected void initializeOptionParser(final OptionParser parser) {
    parser
        .accepts(CLASS_ATTRIBUTES,
            "The list of class attributes to use, for defining the classes")
        .withRequiredArg().ofType(String.class).withValuesSeparatedBy(',');
  }

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

    final Tap<?, ?, ?> source = new Hfs(new WritableSequenceFile(
        Analytics.getHeadFields(BasicClassStats.class), Text.class), input.get(0));

    // Set the class attributes
    if (options.has(CLASS_ATTRIBUTES)) {
      final List<String> classes = (List<String>) options.valuesOf(CLASS_ATTRIBUTES);
      final StringBuilder sb = new StringBuilder();
      for (String c : classes) {
        sb.append(c).append(',');
      }
      properties
          .setProperty(AnalyticsParameters.CLASS_ATTRIBUTES_FIELD.toString(),
              sb.toString());
    } else {
      printMissingOptionError(CLASS_ATTRIBUTES);
    }
    AppProps.setApplicationJarClass(properties, BasicClassStatsCLI.class);
    final BasicClassStats pipe = new BasicClassStats();
    final Flow<?> flow = new HadoopFlowConnector(properties).connect(
        "basic-class-stats", source, new Hfs(new SequenceFile(
            Analytics.getTailFields(BasicClassStats.class)), output.get(0), SinkMode.REPLACE),
        pipe);

    flow.complete();

    return flow;
  }

  /**
   * Runs the basic class statistics.
   * 
   * @param args
   *          Arguments for cascading job. See options in
   *          {@link #initializeOptionParser(OptionParser)
   *          initializeOptionParser}
   * @throws Exception
   *           Exception
   */
  public static void main(final String[] args) throws Exception {
    final BasicClassStatsCLI cli = new BasicClassStatsCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
