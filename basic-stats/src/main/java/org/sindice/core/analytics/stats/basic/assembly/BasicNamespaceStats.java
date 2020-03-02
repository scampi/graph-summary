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
import org.sindice.core.analytics.cascading.operation.PredicateFilter;
import org.sindice.core.analytics.stats.assembly.Json2NamespaceFunction;
import org.sindice.core.analytics.stats.assembly.Predicate2Namespace;
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
@AnalyticsName(value="basic-namespace-stats")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(fields={ "value" })
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "namespace", "objectsNumber", "url", "domain", "sndDomain" })
})
public class BasicNamespaceStats extends AnalyticsSubAssembly {

  private static final long serialVersionUID = 1L;

  private static final Predicate2Namespace pred2space = new Predicate2Namespace();

  public BasicNamespaceStats() {
    super();
  }

  public BasicNamespaceStats(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    // Read files and parse namespace data
    Json2NamespaceFunction function = new Json2NamespaceFunction();
    Pipe pipe = new Each(previous[0], Analytics.getHeadFields(this), function, Fields.RESULTS);

    // Hash data to improve speed of comparisons and reduce size
    pipe = new Each(pipe, new Fields("domain"), new HashFunction(new Fields(
        "domain"), 64), Fields.REPLACE);
    pipe = new Each(pipe, new Fields("sndDomain"), new HashFunction(
        new Fields("sndDomain"), 64), Fields.REPLACE);
    pipe = new Each(pipe, new Fields("url"), new HashFunction(
        new Fields("url"), 64), Fields.REPLACE);

    // Hash the namespace and url for speed in aggregating
    pipe = new Each(pipe, new Fields("namespace", "url"), new HashFunction(
        new Fields("hash-namespace-url"), 64),
        Fields.asDeclaration(new Fields("hash-namespace-url", "namespace",
            "domain", "sndDomain", "objectsNumber")));

    // Aggregate the namespace by url, counting the number of references
    SumBy sumBy = new SumBy(new Fields("objectsNumber"), new Fields(
        "objectsNumber"), Long.class);
    pipe = new AggregateBy(pipe, new Fields("hash-namespace-url"), sumBy,
        new UniqueBy(new Fields("namespace")), new UniqueBy(new Fields(
            "domain")), new UniqueBy(new Fields("sndDomain")));

    // Define the sum functions for number of urls, domains, second domains,
    // and total references to a namespace.
    SumBy totalObj = new SumBy(new Fields("objectsNumber"), new Fields(
        "objectsNumber"), Long.class);
    SumBy totalUrl = new SumBy(new Fields("url"), new Fields("url"),
        Long.class);
    SumBy totalDomain = new SumBy(new Fields("domain"), new Fields("domain"),
        Integer.class);
    SumBy totalSndDomain = new SumBy(new Fields("sndDomain"), new Fields(
        "sndDomain"), Integer.class);

    // Insert a "1" to count the number of urls a namespace appears in
    pipe = new Each(pipe, new Insert(new Fields("url"), 1), Fields.ALL);

    // Hash the namespace and domain for speed in aggregating
    pipe = new Each(pipe, new Fields("namespace", "domain"), new HashFunction(
        new Fields("hash-namespace-domain"), 64),
        Fields.asDeclaration(new Fields("hash-namespace-domain", "namespace",
            "sndDomain", "url", "objectsNumber")));
    // Aggregate the namespace by domain, counting the number of references
    // and urls
    pipe = new AggregateBy(pipe, new Fields("hash-namespace-domain"),
        new UniqueBy(new Fields("sndDomain")), new UniqueBy(new Fields(
            "namespace")), totalObj, totalUrl);

    // Insert a "1" to count the number of domains a namespace appears in
    pipe = new Each(pipe, new Insert(new Fields("domain"), 1), Fields.ALL);

    // Hash the namespace and second domain for speed in aggregating
    pipe = new Each(pipe, new Fields("namespace", "sndDomain"),
        new HashFunction(new Fields("hash-namespace-sndDomain"), 64),
        Fields.asDeclaration(new Fields("hash-namespace-sndDomain",
            "namespace", "domain", "url", "objectsNumber")));
    // Aggregate the namespace by second domain, counting the number of
    // references, urls, and domains.
    pipe = new AggregateBy(pipe, new Fields("hash-namespace-sndDomain"),
        new UniqueBy(new Fields("namespace")), totalDomain, totalObj, totalUrl);

    // Insert a "1" to count the number of second domains a namespace
    // appears in
    pipe = new Each(pipe, new Insert(new Fields("sndDomain"), 1), Fields.ALL);
    // Keep only desired fields
    pipe = new Each(pipe, new Fields("namespace", "domain", "sndDomain",
        "url", "objectsNumber"), new Identity(), Fields.RESULTS);
    // Aggregate the namespace , counting the number of references, urls,
    // domains, and second domains.
    pipe = new AggregateBy(pipe, new Fields("namespace"), totalObj, totalUrl,
        totalDomain, totalSndDomain);

    // Filter certain predicates
    pipe = new Each(pipe, new PredicateFilter(pred2space.getFilters()));

    pipe = new Each(pipe, new Fields("namespace"),
        new Namespace2OriginalURIFunction(pred2space.getNamespace2Uri()),
        Fields.ALL);
    // Keep only desired fields
    pipe = new Each(pipe, new Fields("uri", "objectsNumber", "url", "domain",
        "sndDomain"), new Identity(Analytics.getTailFields(this)), Fields.RESULTS);

    return pipe;
  }

}
