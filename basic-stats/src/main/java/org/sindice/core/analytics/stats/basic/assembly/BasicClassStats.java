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
import org.sindice.core.analytics.stats.assembly.Json2ClassFunction;
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
@AnalyticsName(value="basic-class-stats")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(fields={ "value" })
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "class", "objectsNumber", "url", "domain", "sndDomain" })
})
public class BasicClassStats extends AnalyticsSubAssembly {

  private static final long serialVersionUID = 1L;

  public BasicClassStats() {
    super();
  }

  public BasicClassStats(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    // Read files and parse class data
    Json2ClassFunction function = new Json2ClassFunction();
    Pipe pipe = new Each(previous[0], Analytics.getHeadFields(this), function, Fields.RESULTS);

    // Hash data to improve speed of comparisons and reduce size
    pipe = new Each(pipe, new Fields("domain"), new HashFunction(new Fields(
        "domain"), 64), Fields.REPLACE);
    pipe = new Each(pipe, new Fields("sndDomain"), new HashFunction(
        new Fields("sndDomain"), 64), Fields.REPLACE);
    pipe = new Each(pipe, new Fields("url"), new HashFunction(
        new Fields("url"), 64), Fields.REPLACE);

    // Hash the class and url for speed in aggregating
    pipe = new Each(pipe, new Fields("class", "url"), new HashFunction(
        new Fields("hash-class-url"), 64), Fields.asDeclaration(new Fields(
        "hash-class-url", "class", "domain", "sndDomain", "objectsNumber")));

    // Aggregate the class by url, counting the number of references
    SumBy sumBy = new SumBy(new Fields("objectsNumber"), new Fields(
        "objectsNumber"), Long.class);
    pipe = new AggregateBy(pipe, new Fields("hash-class-url"), sumBy,
        new UniqueBy(new Fields("class")), new UniqueBy(new Fields("domain")),
        new UniqueBy(new Fields("sndDomain")));

    // Define the sum functions for number of urls, domains, second domains,
    // and
    // total references to a class.
    SumBy totalObj = new SumBy(new Fields("objectsNumber"), new Fields(
        "objectsNumber"), Long.class);
    SumBy totalUrl = new SumBy(new Fields("url"), new Fields("url"),
        Long.class);
    SumBy totalDomain = new SumBy(new Fields("domain"), new Fields("domain"),
        Integer.class);
    SumBy totalSndDomain = new SumBy(new Fields("sndDomain"), new Fields(
        "sndDomain"), Integer.class);

    // Insert a "1" to count the number of urls a class appears in
    pipe = new Each(pipe, new Insert(new Fields("url"), 1), Fields.ALL);

    // Hash the class and domain for speed in aggregating
    pipe = new Each(pipe, new Fields("class", "domain"), new HashFunction(
        new Fields("hash-class-domain"), 64), Fields.asDeclaration(new Fields(
        "hash-class-domain", "class", "sndDomain", "url", "objectsNumber")));
    // Aggregate the class by domain, counting the number of references and
    // urls
    pipe = new AggregateBy(pipe, new Fields("hash-class-domain"),
        new UniqueBy(new Fields("sndDomain")), new UniqueBy(
            new Fields("class")), totalObj, totalUrl);

    // Insert a "1" to count the number of domains a class appears in
    pipe = new Each(pipe, new Insert(new Fields("domain"), 1), Fields.ALL);

    // Hash the class and second domain for speed in aggregating
    pipe = new Each(pipe, new Fields("class", "sndDomain"), new HashFunction(
        new Fields("hash-class-sndDomain"), 64),
        Fields.asDeclaration(new Fields("hash-class-sndDomain", "class",
            "domain", "url", "objectsNumber")));
    // Aggregate the class by second domain, counting the number of
    // references,
    // urls, and domains.
    pipe = new AggregateBy(pipe, new Fields("hash-class-sndDomain"),
        new UniqueBy(new Fields("class")), totalDomain, totalObj, totalUrl);

    // Insert a "1" to count the number of second domains a class appears in
    pipe = new Each(pipe, new Insert(new Fields("sndDomain"), 1), Fields.ALL);
    // Keep only desired fields
    pipe = new Each(pipe, new Fields("class", "domain", "sndDomain", "url",
        "objectsNumber"), new Identity(), Fields.RESULTS);
    // Aggregate the class , counting the number of references, urls,
    // domains,
    // and second domains.
    pipe = new AggregateBy(pipe, new Fields("class"), totalObj, totalUrl,
        totalDomain, totalSndDomain);

    // Keep only desired fields
    pipe = new Each(pipe, new Fields("class", "objectsNumber", "url",
        "domain", "sndDomain"), new Identity(Analytics.getTailFields(this)),
        Fields.RESULTS);

    return pipe;
  }

}
