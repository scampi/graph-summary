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
package org.sindice.core.analytics.stats.domain.assembly;

import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.sindice.core.analytics.cascading.operation.HashFunction;
import org.sindice.core.analytics.stats.assembly.Json2DomainFunction;
import org.sindice.core.analytics.stats.assembly.UniqueBy;

import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.tuple.Fields;

/*
 * @author Thomas Perry <thomas.perry@deri.org> [ 24/May/2011 ]
 */
@AnalyticsName(value="list-domains")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(fields={ "value" })
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "domain" })
})
public class ListDomains extends AnalyticsSubAssembly {

  private static final long serialVersionUID = 1L;

  public ListDomains() {
    super();
  }

  public ListDomains(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  /**
   * Generates a list a list of domains from the input documents
   */
  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    // Reads and parses documents
    Json2DomainFunction tripleParser = new Json2DomainFunction(new Fields(
        "domain", "sndDomain"));
    Pipe pipe = new Each(previous[0], Analytics.getHeadFields(this), tripleParser,
      Analytics.getTailFields(this));

    // Hash domain for improved comparison speeds
    pipe = new Each(pipe, Analytics.getTailFields(this), new HashFunction(new Fields(
        "domain-hash"), 64), Fields.ALL);

    // Remove duplicate domains
    pipe = new AggregateBy(pipe, new Fields("domain-hash"),
      new UniqueBy(Analytics.getTailFields(this)));

    // Keep only desired fields
    pipe = new Each(pipe, Analytics.getTailFields(this), new Identity());

    return pipe;
  }

}
