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
import org.sindice.core.analytics.stats.assembly.HashDomainEntity;
import org.sindice.core.analytics.stats.assembly.Json2ExplicitTriplesFunction;
import org.sindice.core.analytics.stats.assembly.UniqueBy;
import org.sindice.core.analytics.stats.domain.aggregators.CountTripleAndEntity;

import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;

/**
 * Counts the number of documents of each domain.
 * 
 * @author Diego Ceccarelli <diego.ceccarelli@deri.org>
 * @link www.di.unipi.it/~ceccarel
 */
@AnalyticsName(value="distinct-triple-per-domain")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(fields={ "value" })
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "domain", "nTotalTriple", "nUniqueTriple", "nUniqueEntity" })
})
public class CountDistinctTriplesPerDomain extends AnalyticsSubAssembly {

  private static final long serialVersionUID = 1L;

  public CountDistinctTriplesPerDomain() {
    super();
  }

  public CountDistinctTriplesPerDomain(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  /**
   * THIS IS NO LONGER IN USE, TOO SLOW, COULD BE OPTIMIZED, Note this does not
   * count entities that appear only as objects, needs to be corrected
   */
  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    // Read and parse docuemtns
    Json2ExplicitTriplesFunction tripleParser = new Json2ExplicitTriplesFunction(
        new Fields("domain", "sndDomain", "url", "subject", "predicate",
            "object", "isRdfOrRdfa"));
    Pipe pipe = new Each(previous[0], tripleParser,
        new Fields("domain", "sndDomain", "url", "subject", "predicate",
            "object"));

    // Must hash with url if blank-node
    pipe = new Each(pipe, new Fields("subject", "url"), new HashDomainEntity(
        new Fields("subject-hash"), 64), Fields.ALL);
    pipe = new Each(pipe, new Fields("domain", "sndDomain", "url",
        "subject-hash", "predicate", "object"), new Identity(new Fields(
        "domain", "sndDomain", "url", "subject", "predicate", "object")),
        Fields.RESULTS);
    // Hash data for compression and improved speed
    pipe = new Each(pipe, new Fields("url"), new HashFunction(
        new Fields("url"), 64), Fields.REPLACE);
    // pipe = new Each(pipe, new Fields("subject"), new HashFunction(new
    // Fields("subject"), 64), Fields.REPLACE);
    pipe = new Each(pipe, new Fields("domain"), new HashFunction(new Fields(
        "domain"), 64), Fields.REPLACE);
    pipe = new Each(pipe, new Fields("subject", "predicate", "object"),
        new HashFunction(new Fields("triple-hash"), 64), new Fields("domain",
            "sndDomain", "url", "subject", "triple-hash"));

    // Define sum functions to count number of explicit triples
    SumBy totalObj = new SumBy(new Fields("nTriple"), new Fields("nTriple"),
        Integer.class);

    // Insert a "1" to count the number of occurrences of each triple
    pipe = new Each(pipe, new Insert(new Fields("nTriple"), 1), Fields.ALL);

    // Hash url and triple-hash for speed in comparison
    pipe = new Each(pipe, new Fields("url", "triple-hash"), new HashFunction(
        new Fields("url-triple-hash"), 64), new Fields("domain", "sndDomain",
        "triple-hash", "url-triple-hash", "nTriple", "subject"));
    // Aggregate by url and triple hash counting number of triples
    pipe = new AggregateBy(pipe, new Fields("url-triple-hash"), new UniqueBy(
        new Fields("triple-hash")), new UniqueBy(new Fields("sndDomain")),
        new UniqueBy(new Fields("domain")),
        new UniqueBy(new Fields("subject")), totalObj);

    // Hash domain and triple-hash for speed in comparisons
    pipe = new Each(pipe, new Fields("domain", "triple-hash"),
        new HashFunction(new Fields("domain-triple-hash"), 64), new Fields(
            "sndDomain", "triple-hash", "domain-triple-hash", "nTriple",
            "subject"));
    // Aggregate by domain and triple hash, counting number of triples
    pipe = new AggregateBy(pipe, new Fields("domain-triple-hash"),
        new UniqueBy(new Fields("triple-hash")), new UniqueBy(new Fields(
            "sndDomain")), new UniqueBy(new Fields("subject")), totalObj);

    // Hash second domain and triple hash to improve comparisons
    pipe = new Each(pipe, new Fields("sndDomain", "triple-hash"),
        new HashFunction(new Fields("sndDom-triple-hash"), 64), new Fields(
            "sndDomain", "triple-hash", "sndDom-triple-hash", "nTriple",
            "subject"));
    // Aggregate by second domain and triple hash, counting the number of
    // triples
    pipe = new AggregateBy(pipe, new Fields("sndDom-triple-hash"),
        new UniqueBy(new Fields("triple-hash")), new UniqueBy(new Fields(
            "sndDomain")), new UniqueBy(new Fields("subject")), totalObj);

    // Hash second second domain to improve speed of comparisons
    pipe = new Each(pipe, new Fields("sndDomain"), new HashFunction(
        new Fields("sndDom-hash"), 64), new Fields("sndDomain", "sndDom-hash",
        "nTriple", "subject"));

    // Insert a "1" to count the number of unqiue triples
    pipe = new Each(pipe, new Insert(new Fields("nUniqueTriple"), 1),
        Fields.ALL);

    // GroupBy second domain and count the number of triples, unique triples,
    // and entities
    pipe = new GroupBy(pipe, new Fields("sndDom-hash"));
    pipe = new Every(pipe, new Fields("sndDomain", "sndDom-hash", "subject",
        "nTriple", "nUniqueTriple"),
        new CountTripleAndEntity(new Fields("sndDomains", "sndDom-hashs",
            "nEntity", "nTriples", "nUniqueTriples")), Fields.RESULTS);

    // Keep only desired fields
    pipe = new Each(pipe, new Fields("sndDomains", "nTriples",
        "nUniqueTriples", "nEntity"), new Identity());

    // Output tuple with the given field declaration
    pipe = new Each(pipe, new Identity(Analytics.getTailFields(this)));

    return pipe;
  }

}
