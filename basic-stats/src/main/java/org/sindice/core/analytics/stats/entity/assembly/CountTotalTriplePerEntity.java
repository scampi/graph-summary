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
package org.sindice.core.analytics.stats.entity.assembly;

import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.sindice.core.analytics.stats.assembly.Json2ExplicitTotalTriplesFunction;
import org.sindice.core.analytics.stats.assembly.UniqueBy;

import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;

/**
 * Compute the total number of triples an entity appears in as subject
 */

@AnalyticsName(value="triple-per-entity")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(fields={ "value" })
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "subject", "count" })
})

public class CountTotalTriplePerEntity extends AnalyticsSubAssembly {

  private static final long     serialVersionUID = 1L;

  public CountTotalTriplePerEntity() {
    super();
  }

  public CountTotalTriplePerEntity(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    // Read and parse each document and outputs "subject",
    // "subject-hash","sndDom-hash","count"
    Pipe pipe = new Each(previous[0], Analytics.getHeadFields(this),
      new Json2ExplicitTotalTriplesFunction());

    // Define sum function to count total triples
    SumBy totalObj = new SumBy(new Fields("count"), new Fields("count"), Long.class);

    // Aggregate the subject by second domain, counting total triples
    pipe = new AggregateBy(pipe, new Fields("sndDom-hash", "subject-hash"), new UniqueBy(new Fields("subject")), totalObj);
    // Aggregate the subject, counting total triples
    pipe = new AggregateBy(pipe, new Fields("subject-hash"), new UniqueBy(new Fields("subject")), totalObj);

    // Keep only desired fields
    pipe = new Each(pipe, Analytics.getTailFields(this), new Identity());
    return pipe;
  }

}
