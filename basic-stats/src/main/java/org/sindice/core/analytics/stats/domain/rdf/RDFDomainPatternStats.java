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
package org.sindice.core.analytics.stats.domain.rdf;

import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

/**
 *`Parses the dump and returns an hash representation of domain, url, subject,
 * predicate ,object, + object string and frequency for each triple.
 * @author Diego Ceccarelli <diego.ceccarelli@deri.org>
 * @link www.di.unipi.it/~ceccarel
 */
@AnalyticsName(value="rdf-triple-stats")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(fields={ "domain", "nUU", "nUB", "nUL", "nBU", "nBB", "nBL", "nTotal" })
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "output" })
})
public class RDFDomainPatternStats extends AnalyticsSubAssembly {

  private static final long serialVersionUID = 1L;

  public RDFDomainPatternStats() {
    super();
  }

  public RDFDomainPatternStats(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    return new Each(previous[0], Analytics.getHeadFields(this),
      new RDFDomainPatternFunction(Analytics.getTailFields(this)),
      Fields.RESULTS);
  }

}
