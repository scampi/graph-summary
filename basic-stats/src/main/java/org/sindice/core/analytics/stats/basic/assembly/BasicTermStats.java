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
package org.sindice.core.analytics.stats.basic.assembly;

import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.sindice.core.analytics.cascading.operation.HashFunction;
import org.sindice.core.analytics.stats.assembly.Json2LiteralTermFunction;
import org.sindice.core.analytics.stats.assembly.UniqueBy;

import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;

/**
 * @author Diego Ceccarelli <diego.ceccarelli@deri.org>
 * @link www.di.unipi.it/~ceccarel
 */
@AnalyticsName(value="basic-term-stats")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(fields={ "value" })
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "term", "total", "graph", "domain-freq", "snd-level-domain-freq" })
})
public class BasicTermStats
extends AnalyticsSubAssembly {

  private static final long serialVersionUID = 1L;

  public BasicTermStats() {
    super();
  }

  public BasicTermStats(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    //Read files and parse term data
    Json2LiteralTermFunction function = new Json2LiteralTermFunction(
        new Fields("term", "domain", "sndDomain", "url", "objectsNumber"));
    Pipe pipe = new Each(previous[0], new Fields("value"), function, Fields.RESULTS);

    //Hash data to improve speed of comparisons and reduce size
    pipe = new Each(pipe, new Fields("domain"), new HashFunction(new Fields(
        "domain"), 64), Fields.REPLACE);
    pipe = new Each(pipe, new Fields("sndDomain"), new HashFunction(
        new Fields("sndDomain"), 64), Fields.REPLACE);
    pipe = new Each(pipe, new Fields("url"), new HashFunction(
        new Fields("url"), 64), Fields.REPLACE);

    //Define the sum functions for number of urls, domains, second domains, and total references to a term.
    SumBy sumBy = new SumBy(new Fields("objectsNumber"), new Fields(
        "objectsNumber"), Long.class);
    SumBy totalObj = new SumBy(new Fields("objectsNumber"), new Fields(
        "objectsNumber"), Long.class);
    SumBy totalUrl = new SumBy(new Fields("url"), new Fields("url"),
        Long.class);
    SumBy totalDomain = new SumBy(new Fields("domain"), new Fields("domain"),
        Integer.class);
    SumBy totalSndDomain = new SumBy(new Fields("sndDomain"), new Fields(
        "sndDomain"), Integer.class);

    //Hash the term and url for speed in aggregating
    pipe = new Each(pipe, new Fields("term", "url"), new HashFunction(
        new Fields("hash-term-url"), 64), Fields.asDeclaration(new Fields(
        "hash-term-url", "term", "domain", "sndDomain", "objectsNumber")));
    //Aggregate the term by url, counting the number of references
    pipe = new AggregateBy(pipe, new Fields("hash-term-url"), sumBy,
        new UniqueBy(new Fields("term"), new Fields("term"), String.class),
        new UniqueBy(new Fields("domain"), new Fields("domain")),
        new UniqueBy(new Fields("sndDomain"), new Fields("sndDomain")));

    //Insert a "1" to count the number of urls a term appears in
    pipe = new Each(pipe, new Insert(new Fields("url"), 1), Fields.ALL);

    //Hash the term and domain for speed in aggregating
    pipe = new Each(pipe, new Fields("term", "domain"), new HashFunction(
        new Fields("hash-term-domain"), 64), Fields.asDeclaration(new Fields(
        "hash-term-domain", "term", "sndDomain", "url", "objectsNumber")));
    //Aggregate the term by domain, counting the number of references and urls
    pipe = new AggregateBy(pipe, new Fields("hash-term-domain"), new UniqueBy(
        new Fields("sndDomain"), new Fields("sndDomain")), new UniqueBy(
        new Fields("term"), new Fields("term"), String.class), totalObj,
        totalUrl);

    //Insert a "1" to count the number of domains a term appears in
    pipe = new Each(pipe, new Insert(new Fields("domain"), 1), Fields.ALL);

    //Hash the term and second domain for speed in aggregating
    pipe = new Each(pipe, new Fields("term", "sndDomain"), new HashFunction(
        new Fields("hash-term-sndDomain"), 64),
        Fields.asDeclaration(new Fields("hash-term-sndDomain", "term",
            "domain", "url", "objectsNumber")));
    //Aggregate the term by second domain, counting the number of references, urls, and domains.
    pipe = new AggregateBy(pipe, new Fields("hash-term-sndDomain"),
        new UniqueBy(new Fields("term"), new Fields("term"), String.class),
        totalDomain, totalObj, totalUrl);

    //Insert a "1" to count the number of second domains a term appears in
    pipe = new Each(pipe, new Insert(new Fields("sndDomain"), 1), Fields.ALL);
    //Keep only desired fields		
    pipe = new Each(pipe, new Fields("term", "domain", "sndDomain", "url",
        "objectsNumber"), new Identity(), Fields.RESULTS);
    //Aggregate the term , counting the number of references, urls, domains, and second domains.
    pipe = new AggregateBy(pipe, new Fields("term"), totalObj, totalUrl,
        totalDomain, totalSndDomain);

    //Keep only desired fields
    pipe = new Each(pipe, new Identity(Analytics.getTailFields(this)), Fields.RESULTS);
    return pipe;
  }

}
