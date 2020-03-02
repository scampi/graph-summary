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

import org.openrdf.model.Literal;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
import org.sindice.graphsummary.cascading.ecluster.ClusteringFactory;
import org.sindice.graphsummary.cascading.entity.CheckAuthority;
import org.sindice.graphsummary.cascading.entity.FilterClasses;
import org.sindice.graphsummary.cascading.entity.StatementFunction;
import org.sindice.graphsummary.cascading.entity.SubjectAggregateBy;
import org.sindice.graphsummary.cascading.properties.SchemaFunction;
import org.sindice.graphsummary.cascading.relationships.SPO2HashInterclassFunction;

/**
 * This class provides the set of counters used to monitor
 * the data graph summary computation at the instance level
 */
public class InstanceCounters {

  /**
   * The group of the Counters relevant to the data graph summary
   * at the instance level
   */
  public static final String INSTANCE_ID = "Analytics - Instance";

  /**
   * Counts the number of classes which are blank nodes.
   * 
   * @see FilterClasses
   */
  public static final String CLASS_BNODE = "CLASS_BNODE";

  /**
   * Counts the number of empty classes.
   * 
   * @see FilterClasses
   */
  public static final String CLASS_EMPTY = "CLASS_EMPTY";

  /**
   * Number of triples in the datasets.
   * 
   * @see StatementFunction
   */
  public static final String TRIPLES = "TRIPLES";

  /**
   * Number of effective triples in the datasets, after some filtering
   * such as {@link FilterClasses} and the ones in {@link StatementFunction}.
   * 
   * @see SubjectAggregateBy
   */
  public static final String EFFECTIVE_TRIPLES = "EFFECTIVE_TRIPLES";

  /**
   * Number of triples removed because the predicate is
   * black listed.
   * 
   * @see StatementFunction
   * @see AnalyticsParameters#PREDICATES_BLACKLIST
   */
  public static final String BLACK_PREDICATES = "BLACK_PREDICATES";

  /**
   * Number of triples removed for one of those reasons:
   * <ul>
   * <li>subject label is empty;</li>
   * <li>predicate label is empty; or</li>
   * <li>object label is empty, if it is not a {@link Literal}.</li>
   * <ul>
   * @see StatementFunction
   */
  public static final String EMPTY_VALUE_IN_TRIPLE = "EMPTY_VALUE_IN_TRIPLE";

  /**
   * Number of entities in the datasets.
   * 
   * <p>
   * 
   * The provenance of the entity is kept.
   * Therefore, an entity &lt;a&gt; in the datasets <b>A</b> and <b>B</b> is
   * considered as 2 entities.
   * 
   * @see SubjectAggregateBy
   */
  public static final String ENTITIES = "ENTITIES";

  /**
   * Number of authoritative entities
   * 
   * @see CheckAuthority
   * @see AnalyticsParameters#CHECK_AUTH_TYPE
   */
  public static final String AUTH_ENTITIES = "AUTH_ENTITIES";

  /**
   * Number of non-authoritative entities
   * 
   * @see CheckAuthority
   * @see AnalyticsParameters#CHECK_AUTH_TYPE
   */
  public static final String NON_AUTH_ENTITIES = "NON_AUTH_ENTITIES";

  /**
   * Number of entities without a cluster
   * 
   * @see ClusteringFactory
   */
  public static final String NO_CLUSTER = "NO_CLUSTER";

  /**
   * Number of relations.
   * 
   * <p>
   * 
   * A relation is a statement where the predicate is not a
   * class attribute, and the object is not a {@link Literal}.
   * 
   * @see SPO2HashInterclassFunction
   */
  public static final String RELATIONS = "RELATIONS";

  /**
   * Number of properties, i.e., triples which predicate is
   * not a class attribute.
   * 
   * @see SchemaFunction
   */
  public static final String PROPERTIES = "PROPERTIES";

  /**
   * Number of triples which predicate is a class attribute.
   * 
   * @see SchemaFunction
   */
  public static final String CLASSES = "CLASSES";

}
