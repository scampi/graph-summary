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
package org.sindice.core.analytics.stats.entity.assembly;

import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.sindice.core.analytics.cascading.operation.HashFunction;
import org.sindice.core.analytics.stats.assembly.ConcatenateUrlToBlankNodeFunction;
import org.sindice.core.analytics.stats.assembly.Json2ExplicitTriplesFunction;
import org.sindice.core.analytics.stats.assembly.UniqueBy;

import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.Unique;
import cascading.tuple.Fields;

/**
 * Compute the number of unique triples an entity appears in as the subject
 */
@AnalyticsName(value="triple-per-entity")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(fields={ "value" })
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "subject", "n-triples" })
})

public class CountTriplePerEntity extends AnalyticsSubAssembly {

  private static final long serialVersionUID = 1L;

  public CountTriplePerEntity() {
    super();
  }

  public CountTriplePerEntity(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  /**
   * Compute the number of unique triples an entity appears in as the subject
   */
  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    // Read and parse input files
    Pipe pipe = new Each(previous[0], Analytics.getHeadFields(this),
        new Json2ExplicitTriplesFunction());
    // Concatenate to url to blank nodes because they are local identifiers
    pipe = new Each(pipe, new Fields("url", "subject"),
        new ConcatenateUrlToBlankNodeFunction(new Fields("url", "subject")),
        Fields.REPLACE);

    // Define hash functions
    Function<?> hashUrlTriple = new HashFunction(
        new Fields("hash-url-triple"), 64);
    Function<?> hashDomainTriple = new HashFunction(new Fields(
        "hash-domain-triple"), 64);
    Function<?> hashSndDomainTriple = new HashFunction(new Fields(
        "hash-sndDomain-triple"), 64);
    Function<?> hashTriple = new HashFunction(new Fields("hash-triple"), 64);

    // Hash data for compression and improved comparisons
    pipe = new Each(pipe, new Fields("url", "subject", "predicate", "object"),
        hashUrlTriple, Fields.ALL);
    pipe = new Each(pipe, new Fields("domain", "subject", "predicate",
        "object"), hashDomainTriple, Fields.ALL);
    pipe = new Each(pipe, new Fields("sndDomain", "subject", "predicate",
        "object"), hashSndDomainTriple, Fields.ALL);
    pipe = new Each(pipe, new Fields("subject", "predicate", "object"),
        hashTriple, Fields.ALL);

    // Aggregate triples by url
    pipe = new AggregateBy(pipe, new Fields("hash-url-triple"), new UniqueBy(
        new Fields("subject")), new UniqueBy(new Fields("hash-triple")),
        new UniqueBy(new Fields("hash-domain-triple")), new UniqueBy(
            new Fields("hash-sndDomain-triple")));

    // Aggregate triples by domain
    pipe = new AggregateBy(pipe, new Fields("hash-domain-triple"),
        new UniqueBy(new Fields("subject")), new UniqueBy(new Fields(
            "hash-triple")), new UniqueBy(new Fields("hash-sndDomain-triple")));

    // Aggregate triples by second domains
    pipe = new AggregateBy(pipe, new Fields("hash-sndDomain-triple"),
        new UniqueBy(new Fields("hash-triple")), new UniqueBy(new Fields(
            "subject")));

    // Keep only unique triples
    pipe = new Unique(pipe, new Fields("subject", "hash-triple"));

    // Count the number of unique triples an entity is the subject of.
    pipe = new GroupBy(pipe, new Fields("subject"));
    pipe = new Every(pipe, new Fields("subject"), new Count(), new Fields(
        "subject", "count"));

    // Output tuple with the given field declaration
    pipe = new Each(pipe, new Identity(Analytics.getTailFields(this)));

    return pipe;
  }

}
