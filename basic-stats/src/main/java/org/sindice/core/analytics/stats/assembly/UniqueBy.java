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
package org.sindice.core.analytics.stats.assembly;

import cascading.flow.FlowProcess;
import cascading.operation.aggregator.First;
import cascading.pipe.assembly.AggregateBy;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * @author diego
 * 
 */
public class UniqueBy extends AggregateBy {

  private static final long serialVersionUID = 1L;

  public static class DistinctCount implements Functor {

    private static final long serialVersionUID = 1L;
    private Fields declaredFields;

    /** Constructor SumPartials creates a new SumPartials instance. */
    public DistinctCount(Fields declaredFields) {
      this.declaredFields = declaredFields;

    }

    @Override
    public Fields getDeclaredFields() {
      return declaredFields;
    }

    @Override
    public Tuple aggregate(FlowProcess flowProcess,
                           TupleEntry arguments,
                           Tuple context) {
      if (context == null) {
        context = arguments.getTupleCopy();
      }
      return context;
    }

    @Override
    public Tuple complete(FlowProcess flowProcess, Tuple context) {
      return context;
    }
  }

  public UniqueBy(Fields valueField, Fields sumField, Class<?> sumType) {
    super(valueField, new DistinctCount(sumField), new First(sumField));
  }

  // FIXME: the type could be removed
  public UniqueBy(Fields valueField, Fields sumField) {
    this(valueField, sumField, Integer.class);
  }

  public UniqueBy(Fields valueField) {
    this(valueField, valueField, Integer.class);
  }

}
