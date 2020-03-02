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
import org.sindice.core.analytics.stats.assembly.Json2ClassPredicateFunction;
import org.sindice.core.analytics.stats.assembly.UniqueBy;
import org.sindice.core.analytics.stats.domain.aggregators.DomainClassAggregator;

import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;

/*
 * @author Thomas Perry <thomas.perry@deri.org> [ 24/May/2011 ]
 */

@AnalyticsName(value="class-per-domain")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(fields={ "value" })
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "domain", "class", "nTotalClass", "nDocClass", "predicates" })
})
public class CountClassPerDomain4Graph extends AnalyticsSubAssembly {

  private static final long serialVersionUID = 1L;

  public CountClassPerDomain4Graph() {
    super();
  }

  public CountClassPerDomain4Graph(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    Json2ClassPredicateFunction tripleParser = new Json2ClassPredicateFunction(
        new Fields("sndDomain", "url", "predicate", "class", "objectsNumber"));

    // Parse json file
    Pipe pipe = new Each(previous[0], tripleParser,
        Fields.RESULTS);
    pipe = new Each(pipe, new Fields("url"), new HashFunction(
        new Fields("url"), 64), Fields.REPLACE);

    // Hash the second domain, subject and predicate together
    SumBy totlObj = new SumBy(new Fields("objectsNumber"), new Fields(
        "total-count"), Long.class);
    SumBy totalObj = new SumBy(new Fields("total-count"), new Fields(
        "total-count"), Long.class);
    SumBy totalDoc = new SumBy(new Fields("doc-count"),
        new Fields("doc-count"), Integer.class);

    pipe = new Each(pipe, new Insert(new Fields("total-count"), 1), Fields.ALL);
    pipe = new Each(pipe, new Fields("class", "predicate", "url"),
        new HashFunction(new Fields("name-url-hash"), 64), new Fields(
            "sndDomain", "class", "name-url-hash", "objectsNumber",
            "total-count", "predicate"));
    pipe = new AggregateBy(pipe, new Fields("name-url-hash"), new UniqueBy(
        new Fields("sndDomain")), new UniqueBy(new Fields("class")),
        new UniqueBy(new Fields("predicate")), totlObj);

    pipe = new Each(pipe, new Insert(new Fields("doc-count"), 1), Fields.ALL);
    pipe = new Each(pipe, new Fields("class", "predicate", "sndDomain"),
        new HashFunction(new Fields("name-domain-hash"), 64), new Fields(
            "sndDomain", "class", "name-domain-hash", "total-count",
            "doc-count", "predicate"));
    pipe = new AggregateBy(pipe, new Fields("name-domain-hash"), new UniqueBy(
        new Fields("sndDomain")), new UniqueBy(new Fields("class")),
        new UniqueBy(new Fields("predicate")), totalObj, totalDoc);

    // Aggregate predicate totals
    pipe = new Each(pipe, new Fields("class", "sndDomain"), new HashFunction(
        new Fields("name-domain-hash1"), 64), new Fields("sndDomain", "class",
        "name-domain-hash1", "total-count", "doc-count", "predicate"));
    pipe = new GroupBy(pipe, new Fields("name-domain-hash1"));
    pipe = new Every(pipe, new DomainClassAggregator(new Fields("sndDomain",
        "class", "total-count", "doc-count", "predicates")), Fields.ALL);

    pipe = new Each(pipe, new Fields("sndDomain", "class", "total-count",
        "doc-count", "predicates"), new Identity());
    pipe = new Each(pipe, new Identity(Analytics.getTailFields(this)));

    return pipe;
  }

}
