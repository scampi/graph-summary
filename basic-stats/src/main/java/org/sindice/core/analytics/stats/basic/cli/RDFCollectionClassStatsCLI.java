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

import java.util.Properties;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;
import org.sindice.core.analytics.cascading.CascadeConfYAMLoader;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.scheme.ExtensionTextLine;
import org.sindice.core.analytics.stats.basic.rdf.RDFCollectionClassStats;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.management.UnitOfWork;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextLine.Compress;
import cascading.stats.CascadingStats;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

public class RDFCollectionClassStatsCLI extends AbstractAnalyticsCLI {

  private static final String DATE = "date";

  @Override
  protected void initializeOptionParser(final OptionParser parser) {
    parser.accepts(DATE, "Date in the form YYYY-MM-DD").withRequiredArg()
        .describedAs("Date").ofType(String.class);
  }

  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(final OptionSet options) throws Exception {
    final Properties properties = cascadeConf.getFlowsConfiguration()
    .get(CascadeConfYAMLoader.DEFAULT_PARAMETERS);
    
    if (options.has(DATE)) {
      properties.setProperty("date", (String) options.valueOf(DATE));
    } else {
      printMissingOptionError(DATE);
    }

    AppProps.setApplicationJarClass(properties, RDFCollectionClassStatsCLI.class);
    final RDFCollectionClassStats pipe = new RDFCollectionClassStats();
    final Tap source = new Hfs(getInputScheme(Analytics.getHeadFields(pipe)), input.get(0));
    final Tap sink = new Hfs(new ExtensionTextLine("nq", Compress.ENABLE), output.get(0), SinkMode.REPLACE);
    final Flow<?> flow = new HadoopFlowConnector(properties)
      .connect(Analytics.getName(pipe), source, sink, pipe);
    flow.complete();

    return flow;
  }

  public static void main(final String[] args) throws Exception {
    final RDFCollectionClassStatsCLI cli = new RDFCollectionClassStatsCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
