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
import java.util.Map.Entry;

import org.apache.hadoop.io.BytesWritable;
import org.openrdf.model.BNode;
import org.openrdf.model.Value;
import org.sindice.core.analytics.rdf.RDFDocument;
import org.sindice.core.analytics.rdf.RDFParser;
import org.sindice.core.analytics.rdf.Triple;
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
 * @author diego
 */
public class Json2ExplicitTotalTriplesFunction
extends BaseOperation<Json2ExplicitTotalTriplesFunction.Context>
implements Function<Json2ExplicitTotalTriplesFunction.Context> {

  private static final long   serialVersionUID = 1L;

  private static final Logger logger           = LoggerFactory.getLogger(Json2ExplicitTotalTriplesFunction.class);

  public Json2ExplicitTotalTriplesFunction(Fields fieldDeclaration) {
    super(fieldDeclaration);
  }

  public Json2ExplicitTotalTriplesFunction() {
    this(new Fields("subject", "subject-hash", "sndDom-hash", "count"));
  }

  public static class Context {
    final Tuple                result   = new Tuple();
    final Map<String, Integer> subjects = new HashMap<String, Integer>();
    final StringBuilder        sb       = new StringBuilder();
    final RDFParser            rdfParser;

    public Context(FlowProcess fp) {
      rdfParser = new RDFParser(fp);
    }
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
      final String url = doc.getUrl();
      final String datasetLabel = doc.getDatasetLabel();
      final Long sndDomHash = Hash.getHash64(datasetLabel);

      c.subjects.clear();
      final List<Triple> triples = doc.getTriples();
      for (Triple s : triples) {
        final Value subject = s.getSubject();
        final String subjectString;
        if (subject instanceof BNode) {
          c.sb.setLength(0);
          subjectString = c.sb.append(s.getSubjectString()).append("|").append(url).toString();
        } else {
          subjectString = s.getSubjectString();
        }
        if (!c.subjects.containsKey(subjectString)) {
          c.subjects.put(subjectString, 1);
        } else {
          c.subjects.put(subjectString, c.subjects.get(subjectString) + 1);
        }
      }

      String subject = null;
      for (Entry<String, Integer> sub : c.subjects.entrySet()) {

        subject = sub.getKey();
        int count = sub.getValue();

        final BytesWritable subHash = Hash.getHash128(subject);
        c.result.clear();
        c.result.addAll(subject, subHash, sndDomHash, count);
        functionCall.getOutputCollector().add(c.result);
      }
    } catch (Exception e) {
      logger.error("Error parsing the json, skipping", e);
    }
  }

}
