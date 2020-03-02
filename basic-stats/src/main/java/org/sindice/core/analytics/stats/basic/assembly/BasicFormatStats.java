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
import org.sindice.core.analytics.stats.assembly.Json2FormatFunction;
import org.sindice.core.analytics.stats.assembly.UniqueBy;

import cascading.operation.Function;
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
@AnalyticsName(value="basic-format-stats")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(fields={ "value" })
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "format", "url", "domain", "sndDomain" })
})
public class BasicFormatStats extends AnalyticsSubAssembly {

  private static final long serialVersionUID = 1L;

  public BasicFormatStats() {
    super();
  }

  public BasicFormatStats(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    // Read files and parse format data
    final Function<?> function = new Json2FormatFunction(new Fields("format",
        "domain", "sndDomain", "url"));
    Pipe pipe = new Each(previous[0], Analytics.getHeadFields(this),
      function, Fields.RESULTS);

    // Hash data to improve speed of comparisons and reduce size
    pipe = new Each(pipe, new Fields("domain"), new HashFunction(new Fields(
        "domain"), 64), Fields.REPLACE);
    pipe = new Each(pipe, new Fields("sndDomain"), new HashFunction(
        new Fields("sndDomain"), 64), Fields.REPLACE);
    pipe = new Each(pipe, new Fields("url"), new Insert(new Fields("url"), 1),
        Fields.REPLACE);

    // Define the sum functions for number of urls, domains, second domains,
    // with the format.
    SumBy totalUrl = new SumBy(new Fields("url"), new Fields("url"),
        Long.class);
    SumBy totalDomain = new SumBy(new Fields("domain"), new Fields("domain"),
        Integer.class);
    SumBy totalSndDomain = new SumBy(new Fields("sndDomain"), new Fields(
        "sndDomain"), Integer.class);

    // Hash the format and domain for speed in aggregating
    pipe = new Each(pipe, new Fields("format", "domain"), new HashFunction(
        new Fields("hash-format-domain"), 64),
        Fields.asDeclaration(new Fields("hash-format-domain", "format",
            "sndDomain", "url")));
    // Aggregate the format by domain, counting the number of urls
    pipe = new AggregateBy(pipe, new Fields("hash-format-domain"),
        new UniqueBy(new Fields("sndDomain")), new UniqueBy(new Fields(
            "format")), totalUrl);

    // Insert a "1" to count the number of domains a format appears in
    pipe = new Each(pipe, new Insert(new Fields("domain"), 1), Fields.ALL);

    // Hash the format and second domain for speed in aggregating
    pipe = new Each(pipe, new Fields("format", "sndDomain"), new HashFunction(
        new Fields("hash-format-sndDomain"), 64),
        Fields.asDeclaration(new Fields("hash-format-sndDomain", "format",
            "domain", "url")));
    // Aggregate the format by domain, counting the number of domains, and
    // suming the urls
    pipe = new AggregateBy(pipe, new Fields("hash-format-sndDomain"),
        new UniqueBy(new Fields("format")), totalDomain, totalUrl);

    // Insert a "1" to count the number of second domains a format appears in
    pipe = new Each(pipe, new Insert(new Fields("sndDomain"), 1), Fields.ALL);

    // Keep only desired fields
    pipe = new Each(pipe, new Fields("format", "domain", "sndDomain", "url"),
        new Identity(), Fields.RESULTS);

    // Aggregate by format, summing the number urls, domains and counting
    // number of second domains
    pipe = new AggregateBy(pipe, new Fields("format"), totalUrl, totalDomain,
        totalSndDomain);
    return pipe;
  }

}
