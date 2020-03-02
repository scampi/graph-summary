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
/**
 * @project siren-ranking
 * @author Campinas Stephane [ 20 Feb 2012 ]
 * @link stephane.campinas@deri.org
 */
package org.sindice.core.analytics.stats.domain;

import cascading.pipe.assembly.AggregateBy;
import cascading.tuple.Fields;

/**
 * 
 */
public class CountTriplesAggregateBy
extends AggregateBy {

  private static final long serialVersionUID = -6072414608280977871L;

  public CountTriplesAggregateBy(Fields sumField) {
    super(sumField, new CountTriplesAggregatePartials(sumField), new CountTriplesAggregate(sumField));
  }

}
