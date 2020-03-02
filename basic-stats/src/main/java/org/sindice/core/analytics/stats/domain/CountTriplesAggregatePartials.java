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
 * @project sparql-editor-servlet
 * @author Campinas Stephane [ 6 Feb 2012 ]
 * @link stephane.campinas@deri.org
 */
package org.sindice.core.analytics.stats.domain;

import cascading.flow.FlowProcess;
import cascading.pipe.assembly.AggregateBy.Functor;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * 
 */
public class CountTriplesAggregatePartials implements Functor {

  private static final long serialVersionUID = 4993273206343127460L;

  private final Fields declaredFields;

  public enum Partials {
    NB_TUPLES
  }

  public CountTriplesAggregatePartials(Fields declaredFields) {
    this.declaredFields = declaredFields;

    if (declaredFields.size() != 3) {
      throw new IllegalArgumentException(
          "declared fields can only have 3 field, got: " + declaredFields);
    }
  }

  @Override
  public Tuple aggregate(FlowProcess flowProcess, TupleEntry args,
      Tuple context) {
    if (context == null) {
      context = args.getTupleCopy();
    } else {
      context.set(1, context.getLong(1) + args.getLong("explicit-triples"));
      context.set(2, context.getLong(2) + args.getLong("implicit-triples"));
      flowProcess.increment(Partials.NB_TUPLES, 1);
    }
    return context;
  }

  @Override
  public Tuple complete(FlowProcess flowProcess, Tuple context) {
    return context;
  }

  @Override
  public Fields getDeclaredFields() {
    return declaredFields;
  }

}
