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
 * Count the number of references and urls a class appears in within each domain
 * 
 * @author Thomas Perry <thomas.perry@deri.org> [ 24/May/2011 ]
 */

@AnalyticsName(value="class-per-domain")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(fields={ "value" })
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "domain", "class", "nTotalClass", "nDocClass" })
})

public class CountClassPerDomain extends AnalyticsSubAssembly {

  private static final long serialVersionUID = 1L;

  public CountClassPerDomain() {
    super();
  }

  public CountClassPerDomain(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  /**
   * Count the number of references and urls a class appears in within each
   * domain
   */
  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    // Read files and parse class data
    Json2ClassFunction classParser = new Json2ClassFunction(new Fields(
        "domain", "sndDomain", "url", "class", "objectsNumber", "isRdfOrRdfa"));
    Pipe pipe = new Each(previous[0], classParser, new Fields(
        "domain", "sndDomain", "url", "class", "objectsNumber"));

    // Define the sum functions for number of urls, total references to a class.
    SumBy totlObj = new SumBy(new Fields("objectsNumber"), new Fields(
        "total-count"), Long.class);
    SumBy totalObj = new SumBy(new Fields("total-count"), new Fields(
        "total-count"), Long.class);
    SumBy totalDoc = new SumBy(new Fields("doc-count"),
        new Fields("doc-count"), Integer.class);

    // Hash URL for compression
    pipe = new Each(pipe, new Fields("url"), new HashFunction(
        new Fields("url"), 64), Fields.REPLACE);

    // Hash the class and domain for speed in aggregating
    pipe = new Each(pipe, new Fields("class", "url"), new HashFunction(
        new Fields("class-url-hash"), 64), new Fields("sndDomain", "class",
        "class-url-hash", "objectsNumber"));
    // Aggregate the class by url, counting the number of references and urls
    pipe = new AggregateBy(pipe, new Fields("class-url-hash"), new UniqueBy(
        new Fields("sndDomain")), new UniqueBy(new Fields("class")), totlObj);

    // Insert a "1" to count the number of urls a class appears in
    pipe = new Each(pipe, new Insert(new Fields("doc-count"), 1), Fields.ALL);

    // Hash the class and second domain for speed in aggregating
    pipe = new Each(pipe, new Fields("class", "sndDomain"), new HashFunction(
        new Fields("class-domain-hash"), 64), new Fields("sndDomain", "class",
        "class-domain-hash", "total-count", "doc-count"));
    // Aggregate the class by second domain, counting the number of references
    // and urls
    pipe = new AggregateBy(pipe, new Fields("class-domain-hash"),
        new UniqueBy(new Fields("sndDomain")), new UniqueBy(
            new Fields("class")), totalObj, totalDoc);

    // Keep only the desired fields
    pipe = new Each(pipe, new Fields("sndDomain", "class", "total-count",
        "doc-count"), new Identity());
    pipe = new Each(pipe, new Identity(Analytics.getTailFields(this)));

    return pipe;
  }

}
