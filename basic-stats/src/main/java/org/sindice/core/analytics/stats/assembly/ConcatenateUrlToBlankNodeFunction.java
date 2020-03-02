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
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * Adds a field with the domain of the uri in object
 */
public class ConcatenateUrlToBlankNodeFunction
extends BaseOperation<StringBuilder>
implements Function<StringBuilder> {

  private static final long serialVersionUID = 8397756000169775827L;
  /** fields to retrieve */
  private static final String[] fields = { "url", "subject" };

  public ConcatenateUrlToBlankNodeFunction() {
    super(1, new Fields(fields));
  }

  public ConcatenateUrlToBlankNodeFunction(final Fields fieldDeclaration) {
    // expects 1 arguments, fail otherwise
    super(1, fieldDeclaration);
  }

  @Override
  public void prepare(FlowProcess flowProcess,
                      OperationCall<StringBuilder> operationCall) {
    operationCall.setContext(new StringBuilder());
  }

  private boolean isBlankNode(String uri) {
    return (uri.length() < 2 ? false : uri.charAt(0) == '_'
        && uri.charAt(1) == ':');
  }

  @Override
  public void operate(FlowProcess flowProcess,
                      FunctionCall<StringBuilder> functionCall) {
    final String url = functionCall.getArguments().getString("url");
    final String subject = functionCall.getArguments().getString("subject");

    final Tuple output = new Tuple();
    if (isBlankNode(subject)) {
      functionCall.getContext().setLength(0);
      functionCall.getContext().append(subject).append("|").append(url);
      output.addAll(url, functionCall.getContext().toString());
    } else {
      output.addAll(url, subject);
    }

    functionCall.getOutputCollector().add(output);
  }

}
