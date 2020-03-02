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
import org.sindice.core.analytics.stats.assembly.Json2ExplicitTriplesFunction;
import org.sindice.core.analytics.stats.assembly.UniqueBy;

import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;

/**
 * Counts the number of references to and documents a predicate appears in
 * within each domain
 * 
 * @author Thomas Perry <thomas.perry@deri.org> [ 24/May/2011 ]
 */
@AnalyticsName(value="predicate-per-domain")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(fields={ "value" })
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "domain", "predicate", "nTotalPred", "nDocPred" })
})
public class CountPredicatePerDomain extends AnalyticsSubAssembly {

  private static final long serialVersionUID = 1L;

  public CountPredicatePerDomain() {
    super();
  }

  public CountPredicatePerDomain(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  /**
   * Count the number of references and urls a predicate appear in within each
   * domain
   */
  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    // Read, parse, document
    Json2ExplicitTriplesFunction tripleParser = new Json2ExplicitTriplesFunction(
        new Fields("domain", "sndDomain", "url", "subject", "predicate",
            "object", "isRdfOrRdfa"));
    Pipe pipe = new Each(previous[0], Analytics.getHeadFields(this), tripleParser,
        new Fields("domain", "sndDomain", "url", "predicate"));

    // Hash data for compression
    pipe = new Each(pipe, new Fields("domain"), new HashFunction(new Fields(
        "domain"), 64), Fields.REPLACE);
    pipe = new Each(pipe, new Fields("url"), new HashFunction(
        new Fields("url"), 64), Fields.REPLACE);

    // Define sum function to count the total references and documents a
    // predicate appears in
    SumBy totalObj = new SumBy(new Fields("total-count"), new Fields(
        "total-count"), Long.class);
    SumBy totalDoc = new SumBy(new Fields("doc-count"),
        new Fields("doc-count"), Integer.class);

    // Insert a "1" to count the total references to a predicate
    pipe = new Each(pipe, new Insert(new Fields("total-count"), 1), Fields.ALL);

    // Hash the predicate and url to improve comparisons
    pipe = new Each(pipe, new Fields("predicate", "url"), new HashFunction(
        new Fields("pred-url-hash"), 64), new Fields("domain", "sndDomain",
        "predicate", "pred-url-hash", "total-count"));
    // Aggregate the predicate by url, counting the number of references
    pipe = new AggregateBy(pipe, new Fields("pred-url-hash"), new UniqueBy(
        new Fields("domain")), new UniqueBy(new Fields("sndDomain")),
        new UniqueBy(new Fields("predicate")), totalObj);

    // Insert a "1" to count the number of documents a predicate appears in
    pipe = new Each(pipe, new Insert(new Fields("doc-count"), 1), Fields.ALL);

    // Hash the predicate and second domain to improve comparisons
    pipe = new Each(pipe, new Fields("predicate", "sndDomain"),
        new HashFunction(new Fields("pred-domain-hash"), 64), new Fields(
            "sndDomain", "predicate", "pred-domain-hash", "total-count",
            "doc-count"));
    // Aggregate predicate by second domain, counting number of references and
    // douments the predicate appears in
    pipe = new AggregateBy(pipe, new Fields("pred-domain-hash"), new UniqueBy(
        new Fields("sndDomain")), new UniqueBy(new Fields("predicate")),
        totalObj, totalDoc);

    // Keep only the desired fields
    pipe = new Each(pipe, new Fields("sndDomain", "predicate", "total-count",
        "doc-count"), new Identity());
    pipe = new Each(pipe, new Identity(Analytics.getTailFields(this)));

    return pipe;
  }

}
