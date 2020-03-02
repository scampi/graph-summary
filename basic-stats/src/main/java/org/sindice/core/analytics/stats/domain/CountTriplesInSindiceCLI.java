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
package org.sindice.core.analytics.stats.domain;

import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;
import org.sindice.core.analytics.cascading.annotation.Analytics;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.management.UnitOfWork;
import cascading.scheme.hadoop.SequenceFile;
import cascading.stats.CascadingStats;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

public class CountTriplesInSindiceCLI extends AbstractAnalyticsCLI {

  public CountTriplesInSindiceCLI() {
    super();
  }

  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(final OptionSet options)
  throws Exception {
    final CountTriplesInSindice count = new CountTriplesInSindice();
    final Tap source = new Hfs(getInputScheme(Analytics.getHeadFields(count)), input.get(0));
    final Tap sink = new Hfs(new SequenceFile(Analytics.getTailFields(count)), output.get(0));
    final Flow<?> flow = new HadoopFlowConnector().connect(
      Analytics.getName(count), source, sink, count);
    flow.start();
    flow.complete();
    return flow;
  }

  public static void main(String[] args)
  throws Exception{
    final CountTriplesInSindiceCLI cli = new CountTriplesInSindiceCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
