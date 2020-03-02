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
package org.sindice.core.analytics.stats.basic.assembly;

import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Adds a field with the domain of the uri in object
 * 
 * @author Diego Ceccarelli <diego.ceccarelli@deri.org>
 * @link www.di.unipi.it/~ceccarel
 */
public class Namespace2OriginalURIFunction
extends BaseOperation<Void>
implements Function<Void> {

  private static final long serialVersionUID = 1L;

  /** fields to retrieve */
  private static final String[] OUTPUT_FIELDS = { "uri" };

  private final Map<String, String> namespace2URI;

  public Namespace2OriginalURIFunction(Map<String, String> map) {
    this(new Fields(OUTPUT_FIELDS), map);
  }

  public Namespace2OriginalURIFunction(final Fields fieldDeclaration,
                                       Map<String, String> map) {
    // expects 1 arguments, fail otherwise
    super(1, fieldDeclaration);
    this.namespace2URI = map;
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Void> functionCall) {
    // get the arguments TupleEntry
    final TupleEntry arguments = functionCall.getArguments();
    // create a Tuple to hold our result values
    Tuple result = null;
    result = new Tuple();
    // Get the object field
    String namespace = arguments.getString("namespace");
    String uri = namespace2URI.get(namespace);
    if (uri != null)
      result.add(uri);
    else
      result.add(namespace);
    functionCall.getOutputCollector().add(result);
    return;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof Namespace2OriginalURIFunction)) {
      return false;
    }
    if (!super.equals(object)) {
      return false;
    }

    final Namespace2OriginalURIFunction filter = (Namespace2OriginalURIFunction) object;
    return namespace2URI.equals(filter.namespace2URI);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = hash * 31 + namespace2URI.hashCode();
    return hash;
  }

}
