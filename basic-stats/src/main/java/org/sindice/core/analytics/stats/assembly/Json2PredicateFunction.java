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

import org.sindice.core.analytics.rdf.RDFDocument;
import org.sindice.core.analytics.rdf.RDFParser;
import org.sindice.core.analytics.rdf.Triple;
import org.sindice.core.analytics.stats.util.RDFUtil;
import org.sindice.core.analytics.util.AnalyticsException;
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
public class Json2PredicateFunction
extends BaseOperation<Json2PredicateFunction.Context>
implements Function<Json2PredicateFunction.Context> {

  private static final Logger  logger           = LoggerFactory
                                                .getLogger(Json2PredicateFunction.class);

  private static final long    serialVersionUID = 1L;

  public static final String[] OUTPUT_FIELDS    = new String[] { "domain",
      "sndDomain", "url", "predicate", "objectsNumber", "isRdfOrRdfa" };

  public Json2PredicateFunction(Fields fieldDeclaration) {
    super(fieldDeclaration);
  }

  public Json2PredicateFunction() {
    this(new Fields(OUTPUT_FIELDS));
  }

  public static class Context {
    final Tuple result = new Tuple();
    final Map<String, Integer> predicates = new HashMap<String, Integer>();
    final RDFParser rdfParser;

    public Context(FlowProcess fp) {
      rdfParser = new RDFParser(fp);
    }
  }

  @Override
  public void prepare(final FlowProcess flowProcess,
                      final OperationCall<Context> operationCall) {
    operationCall.setContext(new Context(flowProcess));
  }

  @Override
  public void operate(final FlowProcess flowProcess,
                      final FunctionCall<Context> functionCall) {
    final Context c = functionCall.getContext();

    try {
      final RDFDocument doc = c.rdfParser.getRDFDocument(functionCall.getArguments());
      final String domain = doc.getDomain();
      final String url = doc.getUrl();
      final String datasetLabel = doc.getDatasetLabel();

      c.predicates.clear();
      triplesToPredicates(flowProcess, c.predicates, doc);
      final boolean isRdfOrRdfa = RDFUtil.isRdfOrRdfa(doc);

      for (Map.Entry<String, Integer> entry : c.predicates.entrySet()) {
        c.result.clear();
        c.result.addAll(domain, datasetLabel, url, entry.getKey(), entry.getValue(), isRdfOrRdfa);
        functionCall.getOutputCollector().add(c.result);
      }
    } catch (AnalyticsException e) {
      logger.error("Error parsing the json, skipping", e);
    }
  }

  /**
   * Process the statements of this document and fills the {@link Map}
   * with the frequency of predicates.
   * @param doc 
   */
  private void triplesToPredicates(final FlowProcess<?> flowProcess,
                                   final Map<String, Integer> predicates,
                                   final RDFDocument doc)
  throws AnalyticsException {
    final List<Triple> triples = doc.getTriples();

    for (Triple t : triples) {
      final String predicate = t.getPredicateString();
      if (predicates.containsKey(predicate)) {
        predicates.put(predicate, predicates.get(predicate) + 1);
      } else {
        predicates.put(predicate, 1);
      }
    }
  }

}
