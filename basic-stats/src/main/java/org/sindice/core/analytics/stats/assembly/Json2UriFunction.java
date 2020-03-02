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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.openrdf.model.URI;
import org.sindice.core.analytics.rdf.RDFDocument;
import org.sindice.core.analytics.rdf.RDFParser;
import org.sindice.core.analytics.rdf.Triple;
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
 * Parse function to get fields codified in json
 */
public class Json2UriFunction
extends BaseOperation<Json2UriFunction.Context>
implements Function<Json2UriFunction.Context> {

  private static final long    serialVersionUID = 1L;

  private static final Logger  logger           = LoggerFactory.getLogger(Json2UriFunction.class);

  public static final String[] OUTPUT_FIELDS    = new String[] { "uri", "domain", "sndDomain", "url", "objectsNumber" };

  class Context {
    final Tuple     tuple = new Tuple();
    final RDFParser rdfParser;

    public Context(FlowProcess fp) {
      rdfParser = new RDFParser(fp);
    }
  }

  public Json2UriFunction(Fields fieldDeclaration) {
    super(fieldDeclaration);
  }

  public Json2UriFunction() {
    this(new Fields(OUTPUT_FIELDS));
  }

  @Override
  public void prepare(final FlowProcess flowProcess, final OperationCall<Context> operationCall) {
    operationCall.setContext(new Context(flowProcess));
  }

  @Override
  public void operate(final FlowProcess flowProcess, final FunctionCall<Context> functionCall) {
    final Context c = functionCall.getContext();

    try {
      final RDFDocument doc = c.rdfParser.getRDFDocument(functionCall.getArguments());
      final String domain = doc.getDomain();
      final String url = doc.getUrl();
      final String datasetLabel = doc.getDatasetLabel();

      final List<Triple> triples = doc.getTriples();
      final Map<String, Integer> counters = new HashMap<String, Integer>();
      for (final Triple t : triples) {
        final Object[] terms = new Object[] { t.getSubject(), t.getPredicate(), t.getObject() };
        for (Object term : terms) {
          if (term instanceof URI) {
            final String uri = term.toString();
            if (counters.containsKey(uri)) {
              counters.put(uri, counters.get(uri) + 1);
            } else {
              counters.put(uri, 1);
            }
          }
        }
      }
      for (Map.Entry<String, Integer> e : counters.entrySet()) {
        c.tuple.clear();
        c.tuple.addAll(e.getKey(), domain, datasetLabel, url, e.getValue());
        functionCall.getOutputCollector().add(c.tuple);
      }
    } catch (Exception e) {
      logger.error("Error parsing the RDF document, skipping", e);
    }
  }

}
