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
import org.sindice.core.analytics.stats.assembly.Json2ExplicitTriplesDomainFunction;
import org.sindice.core.analytics.stats.assembly.UniqueBy;

import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;

/**
 * Count the number of triples and documents within a domain
 * 
 * @author Diego Ceccarelli <diego.ceccarelli@deri.org>
 * @link www.di.unipi.it/~ceccarel
 */
@AnalyticsName(value="triple-per-domain")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(fields={ "value" })
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "domain", "nTriple", "nDoc" })
})
public class CountTriplesPerDomain extends AnalyticsSubAssembly {

  private static final long serialVersionUID = 1L;

  public CountTriplesPerDomain() {
    super();
  }

  public CountTriplesPerDomain(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  /**
   * Count the number of triples and documents within a domain
   */
  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    // Read, parse, and count number of triples in url/document
    Json2ExplicitTriplesDomainFunction tripleParser = new Json2ExplicitTriplesDomainFunction(
        new Fields("sndDomain", "domain-hash", "nTriple"));
    Pipe pipe = new Each(previous[0], Analytics.getHeadFields(this),
      tripleParser, new Fields("sndDomain", "domain-hash", "nTriple"));

    // Define the sum functions to count the number of triples and documents in a domain
    SumBy totalObj = new SumBy(new Fields("nTriple"), new Fields("nTriple"),
        Long.class);
    SumBy totalDoc = new SumBy(new Fields("nDoc"), new Fields("nDoc"),
        Long.class);

    // Insert a "1" to count the number of documents in a domain
    pipe = new Each(pipe, new Insert(new Fields("nDoc"), 1), Fields.ALL);
    // Count the number of documents and triples in a domain
    pipe = new AggregateBy(pipe, new Fields("domain-hash"), new UniqueBy(
        new Fields("sndDomain")), totalObj, totalDoc);

    // Hash the second domain for speed in comparisons
    pipe = new Each(pipe, new Fields("sndDomain"), new HashFunction(
        new Fields("sndDom-hash"), 64), new Fields("sndDomain", "sndDom-hash",
        "nTriple", "nDoc"));
    // Count the number of documents and triples in a second domain
    pipe = new AggregateBy(pipe, new Fields("sndDom-hash"), new UniqueBy(
        new Fields("sndDomain")), totalObj, totalDoc);

    // Keep only the desired fields
    pipe = new Each(pipe, new Fields("sndDomain", "nTriple", "nDoc"),
        new Identity(Analytics.getTailFields(this)));

    return pipe;
  }

}
