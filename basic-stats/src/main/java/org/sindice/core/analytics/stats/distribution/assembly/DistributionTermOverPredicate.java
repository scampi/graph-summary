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
package org.sindice.core.analytics.stats.distribution.assembly;

import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.sindice.core.analytics.stats.assembly.Json2TermPredicateFunction;
import org.sindice.core.analytics.stats.assembly.UniqueBy;

import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;

/**
 * @project analytics
 * @author Thomas Perry <thomas.perry@deri.org>
 */
@AnalyticsName(value="term-over-predicate-distribution")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(fields={ "line" })
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "distinct-freq", "freq", "type" })
})
public class DistributionTermOverPredicate extends AnalyticsSubAssembly {

  private static final long serialVersionUID = 1L;

  public DistributionTermOverPredicate() {
    super();
  }

  public DistributionTermOverPredicate(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  /**
   * Compute the frequency of unique predicate term pairings.
   */
  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    // Read, parse document,
    Pipe pipe = new Each(previous[0], Analytics.getHeadFields(this),
      new Json2TermPredicateFunction(),
      new Fields("sndDomain", "domain", "term", "term-predicate", "freq", "type"));

    // Define sum functions to count the
    SumBy totalRef = new SumBy(new Fields("freq"), new Fields("freq"), Long.class);
    SumBy distinctRef = new SumBy(new Fields("distinct-freq"), new Fields("distinct-freq"), Integer.class);

    // Count the number of occurrences of a unique predicate and term pair by
    // second domain
    pipe = new AggregateBy(pipe, new Fields("sndDomain", "term-predicate"), new UniqueBy(new Fields("type")), new UniqueBy(new Fields("term")), totalRef);
    // Count the number of occurrences of a unique predicate and term pair
    pipe = new AggregateBy(pipe, new Fields("term-predicate"), new UniqueBy(new Fields("type")), new UniqueBy(new Fields("term")), totalRef);

    // Insert a "1" to count the frequency of unique predicate term pairings.
    pipe = new Each(pipe, new Insert(new Fields("distinct-freq"), 1), Fields.ALL);
    // Compute the frequency of unique predicate term pairings.
    pipe = new AggregateBy(pipe, new Fields("term"), distinctRef, totalRef, new UniqueBy(new Fields("type")));

    // Keep only desired fields
    pipe = new Each(pipe, new Fields("distinct-freq", "freq", "type"),
      new Identity(Analytics.getTailFields(this)), Fields.RESULTS);

    return pipe;
  }

}
