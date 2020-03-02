/**
 * Copyright 2011, Campinas Stephane
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * @project siren-ranking
 * @author Campinas Stephane [ 1 Feb 2011 ]
 * @link stephane.campinas@deri.org
 */
package org.sindice.core.analytics.linkanalysis.ding;

import java.util.Map;
import java.util.Properties;

import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
import org.sindice.core.analytics.cascading.scheme.HFileStringKeyComparator;

import cascading.management.UnitOfWork;
import cascading.stats.CascadingStats;

public class DingCLI
extends AbstractAnalyticsCLI {

  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(OptionSet options)
  throws Exception {
    // CASCADE_CONFIG
    final Map<String, Properties> cascadeProperties = cascadeConf.getFlowsConfiguration(
      DingProcess.FLOW_NAME
    );

    final Properties properties = cascadeProperties.get(DingProcess.FLOW_NAME);

    properties.setProperty(AnalyticsParameters.KEY_COMPARATOR.toString(),
      HFileStringKeyComparator.class.getName());
    final DingProcess proc = new DingProcess(properties, input.get(0), output.get(0));
    proc.complete();
//    return proc;
    // TODO DingProcess must be wrapped into ProcessFlow
    return null;
  }

  public static void main(String[] args) throws Exception {
    final DingCLI cli = new DingCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
