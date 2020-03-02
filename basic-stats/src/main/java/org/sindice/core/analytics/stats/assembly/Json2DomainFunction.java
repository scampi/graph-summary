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

import org.sindice.core.analytics.rdf.RDFDocument;
import org.sindice.core.analytics.rdf.RDFParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * @author diego
 */
public class Json2DomainFunction
extends BaseOperation<Json2DomainFunction.Context>
implements Function<Json2DomainFunction.Context> {

  private static final long   serialVersionUID = 1L;

  private static final Logger logger           = LoggerFactory.getLogger(Json2DomainFunction.class);

  class Context {
    final Tuple     tuple = new Tuple();
    final RDFParser rdfParser;

    public Context(FlowProcess fp) {
      rdfParser = new RDFParser(fp);
    }
  }

  public Json2DomainFunction(Fields fieldDeclaration) {
    super(fieldDeclaration);
  }

  public Json2DomainFunction() {
    this(new Fields("domain", "sndDomain"));
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall) {
    operationCall.setContext(new Context(flowProcess));
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Context> functionCall) {
    final Context c = functionCall.getContext();

    try {
      final RDFDocument doc = c.rdfParser.getRDFDocument(functionCall.getArguments());
      final String domain = doc.getDomain();
      final String datasetLabel = doc.getDatasetLabel();

      c.tuple.addAll(domain, datasetLabel);
      functionCall.getOutputCollector().add(c.tuple);
      c.tuple.clear();
    } catch (Exception e) {
      logger.error("Error parsing the json, skipping", e);
    }
  }

}
