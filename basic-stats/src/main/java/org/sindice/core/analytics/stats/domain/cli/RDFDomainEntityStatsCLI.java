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
package org.sindice.core.analytics.stats.domain.cli;

import java.util.Properties;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;
import org.sindice.core.analytics.cascading.CascadeConfYAMLoader;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.stats.domain.rdf.RDFDomainEntityStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.management.UnitOfWork;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextLine;
import cascading.stats.CascadingStats;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

/**
 * @project analytics
 * @author Thomas Perry <thomas.perry@deri.org>
 */
public class RDFDomainEntityStatsCLI extends AbstractAnalyticsCLI {

  private static final String DATE = "date";

  private static final Logger logger = LoggerFactory
      .getLogger(RDFDomainEntityStatsCLI.class);

  @Override
  protected void initializeOptionParser(final OptionParser parser) {
    parser.accepts(DATE, "Date in the form YYYY-MM-DD").withRequiredArg()
        .describedAs("Date").ofType(String.class);
  }

  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(final OptionSet options)
  throws Exception {
    final Properties properties = cascadeConf.getFlowsConfiguration()
    .get(CascadeConfYAMLoader.DEFAULT_PARAMETERS);
    AppProps.setApplicationJarClass(properties, RDFDomainEntityStatsCLI.class);

    String date = null;
    if (options.has(DATE)) {
      date = (String) options.valueOf(DATE);
    } else {
      printMissingOptionError(DATE);
    }

    final JobConf c = new JobConf();
    c.set("date", date);
    properties.put("cascading.hadoop.jobconf", c);

    final RDFDomainEntityStats pipe = new RDFDomainEntityStats();
    final Tap source = new Hfs(getInputScheme(Analytics.getHeadFields(pipe)), input.get(0));
    final Tap sink = new Hfs(new TextLine(Analytics.getTailFields(pipe)), output.get(0),
      SinkMode.REPLACE);

    final Flow<?> flow = new HadoopFlowConnector(properties).connect(
      Analytics.getName(pipe), source, sink, pipe);

    logger.info("Generate rdf for domain entity stats");
    flow.complete();
    return flow;
  }

  public static void main(final String[] args) throws Exception {
    final RDFDomainEntityStatsCLI cli = new RDFDomainEntityStatsCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
