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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.openrdf.model.BNode;
import org.openrdf.model.URI;
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
public class Json2ExplicitEntityFunction
extends BaseOperation<Json2ExplicitEntityFunction.Context>
implements Function<Json2ExplicitEntityFunction.Context> {

  private static final long   serialVersionUID = 1L;

  private static final Logger logger           = LoggerFactory.getLogger(Json2ExplicitEntityFunction.class);

  public Json2ExplicitEntityFunction(Fields fieldDeclaration) {
    super(fieldDeclaration);
  }

  public Json2ExplicitEntityFunction() {
    this(new Fields("sndDomain", "sndDom-hash", "subject-hash"));
  }

  public static class Context {
    final Tuple       result    = new Tuple();
    final Set<String> resources = new HashSet<String>();
    final Tuple       tuple     = new Tuple();
    final RDFParser   rdfParser;

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
      final StringBuilder sb = new StringBuilder();
      final RDFDocument doc = c.rdfParser.getRDFDocument(functionCall.getArguments());
      final String url = doc.getUrl();
      final String datasetLabel = doc.getDatasetLabel();
      final Long sndDomHash = Hash.getHash64(datasetLabel);

      c.resources.clear();
      final List<Triple> triples = doc.getTriples();
      for (Triple t : triples) {
        final Value s = t.getSubject();
        final Value o = t.getObject();
        final String subject;
        final String object;

        if (s instanceof BNode) {
          sb.setLength(0);
          subject = sb.append(t.getSubjectString()).append("|").append(url).toString();
        } else {
          subject = t.getSubjectString();
        }
        c.resources.add(subject);

        if (o instanceof BNode) {
          sb.setLength(0);
          object = sb.append(t.getObjectString()).append("|").append(url).toString();
        } else if (o instanceof URI) {
          object = t.getObjectString();
        } else {
          // Literal
          continue;
        }
        c.resources.add(object);
      }

      for (String res : c.resources) {
        final BytesWritable subjectHash = Hash.getHash128(res);
        c.result.clear();
        c.result.addAll(datasetLabel, sndDomHash, subjectHash);
        functionCall.getOutputCollector().add(c.result);
      }
    } catch (Exception e) {
      logger.error("Error parsing the RDF document, skipping", e);
    }
  }

}
