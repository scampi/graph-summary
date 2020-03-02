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

package org.sindice.graphsummary.cascading;

import org.sindice.graphsummary.cascading.relationships.RelationsGraph;


/**
 * This class provides the set of counters used to monitor
 * the data graph summary computation at the Cascading job level
 */
public class JobCounters {

  /** The group of the Counters relevant to the data graph summary
   * at the Job level
   */
  public static final String JOB_ID = "Analytics - Job";

  /**
   * Number of relations left after the first join
   *
   * @see RelationsGraph
   */
  public static final String JOIN1 = "JOIN1";

  /**
   * Number of relations left after the second join
   *
   * @see RelationsGraph
   */
  public static final String JOIN2 = "JOIN2";

}
