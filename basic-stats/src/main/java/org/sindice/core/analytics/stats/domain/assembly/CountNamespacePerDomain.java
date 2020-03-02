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
import org.sindice.core.analytics.stats.assembly.Json2NamespaceFunction;
import org.sindice.core.analytics.stats.assembly.UniqueBy;

import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;

/**
 * Counts the number of documents of each domain.
 * 
 * @author Thomas Perry <thomas.perry@deri.org> [ 24/May/2011 ]
 */
@AnalyticsName(value="namespace-per-domain")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(fields={ "value" })
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "domain", "namespace", "nTotalName", "nDocName" })
})
public class CountNamespacePerDomain extends AnalyticsSubAssembly {

  private static final long serialVersionUID = 1L;

  public CountNamespacePerDomain() {
    super();
  }

  public CountNamespacePerDomain(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  /**
   * Count the number of references and urls a namespaces appear in within each
   * domain
   */
  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    // Read and parse documents
    Json2NamespaceFunction namespaceParser = new Json2NamespaceFunction(
        new Fields("domain", "sndDomain", "url", "namespace", "objectsNumber",
            "isRdfOrRdfa"));
    Pipe pipe = new Each(previous[0], namespaceParser,
        new Fields("domain", "sndDomain", "url", "namespace", "objectsNumber"));

    // Hash Data to improve compression
    pipe = new Each(pipe, new Fields("domain"), new HashFunction(new Fields(
        "domain"), 64), Fields.REPLACE);
    pipe = new Each(pipe, new Fields("url"), new HashFunction(
        new Fields("url"), 64), Fields.REPLACE);

    // Define sum functions to count number of documents and references to a
    // namespace
    SumBy totlObj = new SumBy(new Fields("objectsNumber"), new Fields(
        "total-count"), Long.class);
    SumBy totalObj = new SumBy(new Fields("total-count"), new Fields(
        "total-count"), Long.class);
    SumBy totalDoc = new SumBy(new Fields("doc-count"),
        new Fields("doc-count"), Integer.class);

    // Hash the namespace and url for speed in comparisons
    pipe = new Each(pipe, new Fields("namespace", "url"), new HashFunction(
        new Fields("name-url-hash"), 64), new Fields("domain", "sndDomain",
        "namespace", "name-url-hash", "objectsNumber"));
    // Aggregate the namespace by url, counting number of references
    pipe = new AggregateBy(pipe, new Fields("name-url-hash"), new UniqueBy(
        new Fields("domain")), new UniqueBy(new Fields("sndDomain")),
        new UniqueBy(new Fields("namespace")), totlObj);

    // Insert a "1" to count the number of urls a namespace appears in
    pipe = new Each(pipe, new Insert(new Fields("doc-count"), 1), Fields.ALL);

    // Hash namespace and second domain to improve comparisons
    pipe = new Each(pipe, new Fields("namespace", "sndDomain"),
        new HashFunction(new Fields("name-domain-hash"), 64), new Fields(
            "sndDomain", "namespace", "name-domain-hash", "total-count",
            "doc-count"));
    // Aggregate the namespace by second domain couting the number of references
    // and urls it appears in
    pipe = new AggregateBy(pipe, new Fields("name-domain-hash"), new UniqueBy(
        new Fields("sndDomain")), new UniqueBy(new Fields("namespace")),
        totalObj, totalDoc);

    // Keep only desired fields
    pipe = new Each(pipe, new Fields("sndDomain", "namespace", "total-count",
        "doc-count"), new Identity());
    pipe = new Each(pipe, new Identity(Analytics.getTailFields(this)));

    return pipe;
  }

}
