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
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.sindice.core.analytics.rdf.RDFDocument;
import org.sindice.core.analytics.rdf.RDFParser;
import org.sindice.core.analytics.rdf.Triple;
import org.sindice.core.analytics.stats.util.LiteralTokenizer;
import org.sindice.core.analytics.util.Hash;
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
public class Json2TermPredicateFunction
extends BaseOperation<Json2TermPredicateFunction.Context>
implements Function<Json2TermPredicateFunction.Context> {

  private static final Logger  logger           = LoggerFactory.getLogger(Json2TermPredicateFunction.class);

  public static final Integer  BLANK_NODE_TYPE  = 2;
  public static final Integer  LITERAL_TYPE     = 0;
  public static final Integer  URI_TYPE         = 1;

  private static final long    serialVersionUID = 1L;

  public static final String[] OUTPUT_FIELDS    = new String[] { "sndDomain", "domain", "term", "term-predicate",
      "freq", "type"                           };

  class Context {
    final Tuple     tuple = new Tuple();
    final RDFParser rdfParser;

    public Context(FlowProcess fp) {
      rdfParser = new RDFParser(fp);
    }
  }

  public Json2TermPredicateFunction(Fields fieldDeclaration) {
    super(fieldDeclaration);
  }

  public Json2TermPredicateFunction() {
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
      final String datasetLabel = doc.getDatasetLabel();
      final long domainHash = Hash.getHash64(domain);
      final long sndDomHash = Hash.getHash64(datasetLabel);

      // Read document as triples
      final List<Triple> triples = doc.getTriples();
      for (final Triple t : triples) {
        final String predicate = t.getPredicateString();

        if (t.getObject() instanceof Literal) {
          final Map<String, Integer> terms = LiteralTokenizer.getTerms((Literal) t.getObject());
          for (String term : terms.keySet()) {
            final long termHash = Hash.getHash64(term);
            final BytesWritable pt = Hash.getHash128(predicate, term);

            c.tuple.clear();
            c.tuple.addAll(sndDomHash, domainHash, termHash, pt, 1, LITERAL_TYPE);
            functionCall.getOutputCollector().add(c.tuple);
          }
        }
        if (t.getObject() instanceof URI) {
          final String term = t.getObjectString();
          final long termHash = Hash.getHash64(term);
          final BytesWritable pt = Hash.getHash128(predicate, term);

          c.tuple.clear();
          c.tuple.addAll(sndDomHash, domainHash, termHash, pt, 1, URI_TYPE);
          functionCall.getOutputCollector().add(c.tuple);
        }
      }
    } catch (Exception e) {
      logger.error("Error parsing the json, skipping", e);
    }
  }

}
