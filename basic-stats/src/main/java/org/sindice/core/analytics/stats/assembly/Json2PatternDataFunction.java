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

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.sindice.core.analytics.rdf.RDFDocument;
import org.sindice.core.analytics.rdf.RDFParser;
import org.sindice.core.analytics.rdf.Triple;
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
 * Parse function to get fields codified in json, produce data for simple stats
 * on domains: -sndDomain -domain -url -pattern -#pattern
 */
public class Json2PatternDataFunction
extends BaseOperation<Json2PatternDataFunction.Context>
implements Function<Json2PatternDataFunction.Context> {

  private static final Logger  logger           = LoggerFactory.getLogger(Json2PatternDataFunction.class);

  private static final long    serialVersionUID = 1L;

  public static final String[] OUTPUT_FIELDS    = new String[] { "sndDomain",
      "domain", "url", "nUU", "nUB", "nUL", "nBU", "nBB", "nBL" };

  public static class Context {
    final Tuple                result     = new Tuple();
    final Map<String, Integer> patternMap = new HashMap<String, Integer>();
    final StringBuilder        pattern    = new StringBuilder();
    final RDFParser            rdfParser;

    public Context(FlowProcess fp) {
      rdfParser = new RDFParser(fp);
    }
  }

  public Json2PatternDataFunction(Fields fieldDeclaration) {
    super(fieldDeclaration);
  }

  public Json2PatternDataFunction() {
    this(new Fields(OUTPUT_FIELDS));
  }

  @Override
  public void prepare(final FlowProcess flowProcess,
                      final OperationCall<Context> operationCall) {
    operationCall.setContext(new Context(flowProcess));
  }

  private String getType(Value r)
  throws AnalyticsException {
    if (r instanceof Literal) {
      return "L";
    } else if (r instanceof URI) {
      return "U";
    } else if (r instanceof BNode) {
      return "B";
    } else {
      logger.error("No type found for object " + r.toString());
      throw new AnalyticsException("No type found for object " + r.toString());
    }
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

      final List<Triple> triples = doc.getTriples();
      String patternKey;
      int nPattern;

      for (Triple t : triples) {
        try {
          patternKey = "";
          nPattern = 0;

          c.pattern.setLength(0);
          c.pattern.append(getType(t.getSubject()));
          c.pattern.append(getType(t.getObject()));
          patternKey = c.pattern.toString();
          if (c.patternMap.containsKey(patternKey)) {
            nPattern = c.patternMap.get(patternKey);
            c.patternMap.put(patternKey, nPattern + 1);
          } else {
            c.patternMap.put(patternKey, 1);
          }
        } catch (AnalyticsException e) {
          logger.error("Error parsing the json, skipping the line ");
        }
      }
      if (!c.patternMap.isEmpty()) {
        c.result.addAll(datasetLabel, domain, url,
            c.patternMap.get("UU"), c.patternMap.get("UB"),
            c.patternMap.get("UL"), c.patternMap.get("BU"),
            c.patternMap.get("BB"), c.patternMap.get("BL"));
        functionCall.getOutputCollector().add(c.result);
      }
      c.result.clear();
      c.patternMap.clear();
    } catch (Exception e) {
      logger.error("Error parsing the RDF document, skipping", e);
    }
  }

}
