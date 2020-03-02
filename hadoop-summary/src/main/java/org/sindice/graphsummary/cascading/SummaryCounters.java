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

import org.sindice.graphsummary.cascading.hash2value.rdf.RDFMetadataGraph;
import org.sindice.graphsummary.cascading.hash2value.rdf.RDFRelationsGraph;
import org.sindice.graphsummary.cascading.properties.ClusterIDAggregate;
import org.sindice.graphsummary.cascading.relationships.RelationsGraph;

/**
 * This class provides the set of counters used to monitor
 * the data graph summary computation at the summary level
 */
public class SummaryCounters {

  /**
   * The group of the Counters relevant to the data graph summary
   * at the summary level
   */
  public static final String SUMMARY_ID = "Analytics - Summary";

  /**
   * Number of clusters
   *
   * @see ClusterIDAggregate
   */
  public static final String CLUSTERS = "CLUSTERS";

  /**
   * Number of relations between clusters
   *
   * @see RelationsGraph
   */
  public static final String RELATIONS = "RELATIONS";

  /**
   * Number of triples in the summary
   *
   * @see RDFMetadataGraph
   * @see RDFRelationsGraph
   */
  public static final String TRIPLES = "TRIPLES";

}
