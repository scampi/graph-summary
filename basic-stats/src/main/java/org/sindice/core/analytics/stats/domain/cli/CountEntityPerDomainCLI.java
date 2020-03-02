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

import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;
import org.sindice.core.analytics.cascading.CascadeConfYAMLoader;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.stats.domain.assembly.CountEntityPerDomain;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.management.UnitOfWork;
import cascading.property.AppProps;
import cascading.scheme.hadoop.SequenceFile;
import cascading.stats.CascadingStats;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

/**
 * @project analytics
 * @author Thomas Perry <thomas.perry@deri.org>
 */
public class CountEntityPerDomainCLI
extends AbstractAnalyticsCLI {

  /**
   * Count the number of unique entities are referenced within a domain each
   * domain
   */
  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(final OptionSet options)
  throws Exception {
    final Properties properties = cascadeConf.getFlowsConfiguration()
    .get(CascadeConfYAMLoader.DEFAULT_PARAMETERS);
    AppProps.setApplicationJarClass(properties, CountEntityPerDomainCLI.class);

    final CountEntityPerDomain pipe = new CountEntityPerDomain();
    final Tap source = new Hfs(getInputScheme(Analytics.getHeadFields(pipe)), input.get(0));
    final Tap sink = new Hfs(new SequenceFile(Analytics.getTailFields(pipe)),
      output.get(0), SinkMode.REPLACE);

    final Flow<?> flow = new HadoopFlowConnector(properties)
    .connect(Analytics.getName(pipe), source, sink, pipe);

    // block until the flow completes
    flow.complete();
    return flow;
  }

  public static void main(final String[] args)
  throws Exception {
    final CountEntityPerDomainCLI cli = new CountEntityPerDomainCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
