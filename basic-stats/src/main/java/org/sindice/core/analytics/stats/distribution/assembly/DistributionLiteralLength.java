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
import org.sindice.core.analytics.cascading.operation.HashFunction;
import org.sindice.core.analytics.stats.assembly.Json2LiteralLengthFunction;
import org.sindice.core.analytics.stats.assembly.UniqueBy;

import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;

/**
 * Calculate the frequency of literals by number of terms
 * 
 * @project analytics
 * @author Thomas Perry <thomas.perry@deri.org>
 */

@AnalyticsName(value="literal-length-distribution")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(fields={ "line" })
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "nTerms", "freq" })
})

public class DistributionLiteralLength extends AnalyticsSubAssembly {

  private static final long serialVersionUID = 1L;

  public DistributionLiteralLength() {
    super();
  }

  public DistributionLiteralLength(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  /**
   * Calculate the frequency of literals by number of terms
   */
  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    // Read and parse documents, output number of terms in a literal
    Json2LiteralLengthFunction lengthParser = new Json2LiteralLengthFunction(
        new Fields("domain", "sndDomain", "url", "nTerms"));
    Pipe pipe = new Each(previous[0], Analytics.getHeadFields(this), lengthParser,
        new Fields("domain", "sndDomain", "url", "nTerms"));

    // Define sum function to count the number of times each literal length
    // occurs
    SumBy sum = new SumBy(new Fields("freq"), new Fields("freq"), Long.class);

    // Insert a "1" to count the number of occurrences of each literal length
    pipe = new Each(pipe, new Insert(new Fields("freq"), 1), Fields.ALL);

    // Hash the url and literal length to improve comparisons
    pipe = new Each(pipe, new Fields("url", "nTerms"), new HashFunction(
        new Fields("url-term-hash"), 64), new Fields("sndDomain", "domain",
        "url-term-hash", "nTerms", "freq"));
    // Count the number of occurrences of each literal length within a document
    pipe = new AggregateBy(pipe, new Fields("url-term-hash"), new UniqueBy(
        new Fields("sndDomain")), new UniqueBy(new Fields("domain")),
        new UniqueBy(new Fields("nTerms")), sum);

    // Hash the domain and literal length to improve comparisons
    pipe = new Each(pipe, new Fields("domain", "nTerms"), new HashFunction(
        new Fields("domain-term-hash"), 64), new Fields("sndDomain",
        "domain-term-hash", "nTerms", "freq"));
    // Count the number of occurrences of each literal length within a domain
    pipe = new AggregateBy(pipe, new Fields("domain-term-hash"), new UniqueBy(
        new Fields("sndDomain")), new UniqueBy(new Fields("nTerms")), sum);

    // Hash the second domain and literal length to improve comparisons
    pipe = new Each(pipe, new Fields("sndDomain", "nTerms"), new HashFunction(
        new Fields("sndDom-term-hash"), 64), new Fields("sndDom-term-hash",
        "nTerms", "freq"));
    // Count the number of occurrences of each literal length within a second
    // document
    pipe = new AggregateBy(pipe, new Fields("sndDom-term-hash"), new UniqueBy(
        new Fields("nTerms")), sum);

    // Count the number of occurrences of each literal length
    pipe = new AggregateBy(pipe, new Fields("nTerms"), sum);

    // Keep only desired fields
    pipe = new Each(pipe, Analytics.getTailFields(this), new Identity());

    return pipe;
  }

}
