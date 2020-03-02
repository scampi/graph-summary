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
import org.sindice.core.analytics.stats.assembly.Json2PatternDataFunction;
import org.sindice.core.analytics.stats.assembly.UniqueBy;

import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;

/**
 * Count the occurrences of triple patterns within a domain
 * 
 * @author Thomas Perry <thomas.perry@deri.org> [ 24/May/2011 ]
 */
@AnalyticsName(value="pattern-per-domain")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(fields={ "value" })
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "domain", "nUU", "nUB", "nUL", "nBU", "nBB", "nBL", "nTotal" })
})
public class CountPatternPerDomain extends AnalyticsSubAssembly {

  private static final long serialVersionUID = 1L;

  public CountPatternPerDomain() {
    super();
  }

  public CountPatternPerDomain(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  /**
   * Count the occurrences of triple patterns within a domain
   */
  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    // Read, parse docuemtns, Count number of triple patterns within document
    Json2PatternDataFunction classParser = new Json2PatternDataFunction(
        new Fields("sndDomain", "domain", "url", "nUU", "nUB", "nUL", "nBU",
            "nBB", "nBL"));
    Pipe pipe = new Each(previous[0], classParser, new Fields(
        "sndDomain", "domain", "nUU", "nUB", "nUL", "nBU", "nBB", "nBL"));

    // Hash domain for compression
    pipe = new Each(pipe, new Fields("domain"), new HashFunction(new Fields(
        "domain"), 64), Fields.REPLACE);

    // Define sum functions to count the occurrence of each triple pattern
    SumBy sumUU = new SumBy(new Fields("nUU"), new Fields("nUU"),
        Integer.class);
    SumBy sumUB = new SumBy(new Fields("nUB"), new Fields("nUB"),
        Integer.class);
    SumBy sumUL = new SumBy(new Fields("nUL"), new Fields("nUL"),
        Integer.class);
    SumBy sumBU = new SumBy(new Fields("nBU"), new Fields("nBU"),
        Integer.class);
    SumBy sumBB = new SumBy(new Fields("nBB"), new Fields("nBB"),
        Integer.class);
    SumBy sumBL = new SumBy(new Fields("nBL"), new Fields("nBL"),
        Integer.class);

    // Aggregate the triple patterns by domain, counting the occurrence of each pattern
    pipe = new AggregateBy(pipe, new Fields("domain"), new UniqueBy(
        new Fields("sndDomain")), sumUU, sumUB, sumUL, sumBU, sumBB, sumBL);

    // Hash second domain for speed in comparison
    pipe = new Each(pipe, new Fields("sndDomain"), new HashFunction(
        new Fields("sndDom-hash"), 64), new Fields("sndDomain", "sndDom-hash",
        "nUU", "nUB", "nUL", "nBU", "nBB", "nBL"));
    // Aggregate triple patterns by second domain, counting the occurrences of each pattern
    pipe = new AggregateBy(pipe, new Fields("sndDom-hash"), new UniqueBy(
        new Fields("sndDomain")), sumUU, sumUB, sumUL, sumBU, sumBB, sumBL);

    // Sum all the patterns to get total triples
    pipe = new Each(pipe,
        new Fields("nUU", "nUB", "nUL", "nBU", "nBB", "nBL"),
        new AddValuesFunction(new Fields("nTotal")), new Fields("sndDomain",
            "nUU", "nUB", "nUL", "nBU", "nBB", "nBL", "nTotal"));

    // Output tuple with the given field declaration
    pipe = new Each(pipe, new Fields("sndDomain", "nUU", "nUB", "nUL", "nBU",
        "nBB", "nBL", "nTotal"), new Identity(Analytics.getTailFields(this)));

    return pipe;
  }

}
