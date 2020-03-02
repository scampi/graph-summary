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
package org.sindice.core.analytics.stats.domain.assembly;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Given the list of integers. The function returns the sum of the values
 * 
 * @author Thomas Perry <thomas.perry@deri.org> [ 24/May/2011 ]
 */
public class AddValuesFunction
extends BaseOperation<Void>
implements Function<Void> {

  private static final long serialVersionUID = 1L;

  /** fields to retrieve */
  private static final String[] fields = { "sum" };

  public AddValuesFunction() {
    super(2, new Fields(fields));
  }

  public AddValuesFunction(Fields fieldDeclaration) {
    super(2, fieldDeclaration);
  }

  /**
   * Sum all the values provided
   */
  public void operate(FlowProcess flowProcess,
                      FunctionCall<Void> functionCall) {
    final TupleEntry arguments = functionCall.getArguments();
    Tuple result = new Tuple();

    int nArgument = arguments.size();
    long sum = 0;

    // Sum all the arguments
    for (int i = 0; i < nArgument; i++) {
      sum += arguments.getLong(i);
    }

    // add the sum value to the result Tuple
    result.add(sum);

    // return the result Tuple
    functionCall.getOutputCollector().add(result);
  }

}
