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
import org.sindice.core.analytics.stats.assembly.Json2ExplicitEntityFunction;
import org.sindice.core.analytics.stats.assembly.UniqueBy;

import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;

/**
 * Count the number of unique entities are referenced within a domain each
 * domain
 * 
 * @author Thomas Perry <thomas.perry@deri.org> [ 24/May/2011 ]
 */
@AnalyticsName(value="entity-per-domain")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(fields={ "value" })
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "domain", "nUniqueEntity" })
})
public class CountEntityPerDomain extends AnalyticsSubAssembly {

  private static final long serialVersionUID = 1L;

  public CountEntityPerDomain() {
    super();
  }

  public CountEntityPerDomain(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  /**
   * Count the number of unique entities are referenced within a domain each
   * domain
   */
  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    // Read file and parse, Also hash values,
    Json2ExplicitEntityFunction tripleParser = new Json2ExplicitEntityFunction(
        new Fields("sndDomain", "sndDom-hash", "subject-hash"));
    Pipe pipe = new Each(previous[0], tripleParser,
        new Fields("sndDomain", "sndDom-hash", "subject-hash"));

    // Keep only unique entities in a domain
    pipe = new AggregateBy(pipe, new Fields("sndDom-hash", "subject-hash"),
        new UniqueBy(new Fields("sndDomain")));

    // Insert a "1" to count the number of unique entities
    pipe = new Each(pipe, new Insert(new Fields("nUniqueEntity"), 1),
        Fields.ALL);
    // Count the number of unique entities
    SumBy count = new SumBy(new Fields("nUniqueEntity"), new Fields(
        "nUniqueEntity"), Long.class);
    pipe = new AggregateBy(pipe, new Fields("sndDom-hash"), new UniqueBy(
        new Fields("sndDomain")), count);

    // Output tuple with the given field declaration
    pipe = new Each(pipe, new Fields("sndDomain", "nUniqueEntity"),
        new Identity(Analytics.getTailFields(this)));

    return pipe;
  }

}
