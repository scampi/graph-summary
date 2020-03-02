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
package org.sindice.core.analytics.stats.basic.rdf;

import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.sindice.core.analytics.stats.basic.assembly.BasicFormatStats;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

@AnalyticsName(value="rdf-format-stats")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(from=BasicFormatStats.class)
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "rdf" })
})

public class RDFCollectionFormatStats extends AnalyticsSubAssembly {

  private static final long serialVersionUID = 1L;

  public RDFCollectionFormatStats() {
    super();
  }

  public RDFCollectionFormatStats(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    Pipe pipe = new Each(previous[0],
      new RDFCollectionFormatFunction(Analytics.getTailFields(this)),
      Fields.RESULTS);
    return pipe;
  }

}
