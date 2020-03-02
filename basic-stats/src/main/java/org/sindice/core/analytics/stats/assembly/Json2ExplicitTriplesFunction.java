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

import java.util.List;

import org.sindice.core.analytics.rdf.RDFDocument;
import org.sindice.core.analytics.rdf.RDFParser;
import org.sindice.core.analytics.rdf.Triple;
import org.sindice.core.analytics.stats.util.RDFUtil;
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
public class Json2ExplicitTriplesFunction
extends BaseOperation<Json2ExplicitTriplesFunction.Context>
implements Function<Json2ExplicitTriplesFunction.Context> {

  private static final long   serialVersionUID = 1L;

  private static final Logger logger           = LoggerFactory.getLogger(Json2ExplicitTriplesFunction.class);

  class Context {
    final Tuple     tuple = new Tuple();
    final RDFParser rdfParser;

    public Context(FlowProcess fp) {
      rdfParser = new RDFParser(fp);
    }
  }

  public Json2ExplicitTriplesFunction(Fields fieldDeclaration) {
    super(fieldDeclaration);
  }

  public Json2ExplicitTriplesFunction() {
    this(new Fields("domain", "sndDomain", "url", "subject", "predicate", "object", "isRdfOrRdfa"));
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
      final String url = doc.getUrl();
      final String datasetLabel = doc.getDatasetLabel();

      final List<Triple> triples = doc.getTriples();
      final boolean isRdfOrRdfa = RDFUtil.isRdfOrRdfa(doc);

      for (Triple t : triples) {
        c.tuple.clear();
        c.tuple.addAll(domain, datasetLabel, url, t.getSubject().toString(), t.getPredicate()
        .toString(), t.getObject().toString(), isRdfOrRdfa);
        functionCall.getOutputCollector().add(c.tuple);
      }
    } catch (Exception e) {
      logger.error("Error parsing the json, skipping", e);
    }
  }

}
