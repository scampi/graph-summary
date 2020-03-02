/**
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
 */

package org.sindice.graphsummary.cascading.ecluster;

import static org.sindice.core.analytics.util.AnalyticsCounters.TUPLES_READ;
import static org.sindice.core.analytics.util.AnalyticsCounters.TUPLES_WRITTEN;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.sindice.graphsummary.cascading.entity.CheckAuthority;
import org.sindice.graphsummary.cascading.entity.GetClusterGraph;

import cascading.operation.state.Counter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;

/**
 * Abstract implementation of a clustering algorithm.
 * 
 * <p>
 * 
 * This class wraps an implementation {@link SubAssembly}
 * of a clustering algorithm.
 * This adds the following pre-processing steps:
 * <ul>
 * <li>{@link CheckAuthority}</li>
 * </ul>
 * 
 * <p>
 * 
 * The tuple schema is the following:<br>
 * 
 * <ul>
 * <li>
 * <b>Input</b>:
 * 
 * [ domain | subject-hash | spo ]
 * </li>
 * <li>
 * <b>Output</b>:
 * 
 * [ domain | subject-hash | spo | cluster-id ]
 * </li>
 */
@AnalyticsName(value="cluster-generator")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(from=GetClusterGraph.class)
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "domain", "subject-hash", "spo-in", "spo-out", "cluster-id" })
})
public abstract class ClusterSubAssembly
extends AnalyticsSubAssembly {

  private static final long serialVersionUID = 2230500092196578762L;

  public ClusterSubAssembly() {
    super();
  }

  public ClusterSubAssembly(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  public ClusterSubAssembly(Object... args) {
    super(args);
  }

  public ClusterSubAssembly(Pipe[] pipes, Object...args) {
    super(pipes, args);
  }

  /**
   * This method returns a {@link SubAssembly} which implement a clustering
   * algorithm.
   * @param entity the {@link Pipe} with information related to an entity
   * @param args the arguments of the clustering algorithm
   */
  protected abstract Pipe cluster(Pipe entity, Object... args);

  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    Pipe entity = new Each(previous[0], new Counter(JOB_ID, TUPLES_READ + name));

    entity = new Each(entity, new CheckAuthority());
    entity = cluster(entity, args);

    entity = new Each(entity, new Counter(JOB_ID, TUPLES_WRITTEN + name));
    return entity;
  }

}
