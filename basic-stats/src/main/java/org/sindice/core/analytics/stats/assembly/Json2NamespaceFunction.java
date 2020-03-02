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

import org.apache.hadoop.mapred.JobConf;
import org.openrdf.model.BNode;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
import org.sindice.core.analytics.rdf.AnalyticsClassAttributes;
import org.sindice.core.analytics.rdf.RDFDocument;
import org.sindice.core.analytics.rdf.RDFParser;
import org.sindice.core.analytics.rdf.Triple;
import org.sindice.core.analytics.stats.util.RDFUtil;
import org.sindice.core.analytics.util.AnalyticsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * @author diego
 */
public class Json2NamespaceFunction
extends BaseOperation<Json2NamespaceFunction.Context>
implements Function<Json2NamespaceFunction.Context> {

  private static final Logger  logger           = LoggerFactory
                                                .getLogger(Json2NamespaceFunction.class);

  private static final long serialVersionUID = 1L;

  public static final String[] OUTPUT_FIELDS = new String[] { "domain",
      "sndDomain", "url", "namespace", "objectsNumber", "isRdfOrRdfa" };

  public Json2NamespaceFunction(Fields fieldDeclaration) {
    super(fieldDeclaration);
  }

  public Json2NamespaceFunction() {
    this(new Fields(OUTPUT_FIELDS));
  }

  public static class Context {
    final Tuple                result     = new Tuple();
    final Map<String, Integer> namespaces = new HashMap<String, Integer>();
    final Predicate2Namespace  pred2space = new Predicate2Namespace();
    final RDFParser            rdfParser;

    public Context(FlowProcess fp) {
      rdfParser = new RDFParser(fp);
    }
  }

  @Override
  public void prepare(final FlowProcess flowProcess,
                      final OperationCall<Context> operationCall) {
    final JobConf conf = ((HadoopFlowProcess) flowProcess).getJobConf();

    // Class Attributes
    final String[] classAttributes = conf
    .getStrings(AnalyticsParameters.CLASS_ATTRIBUTES_FIELD.toString(),
      AnalyticsParameters.CLASS_ATTRIBUTES_FIELD.get());
    AnalyticsClassAttributes.initClassAttributes(classAttributes);
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
      final boolean isRdfOrRdfa = RDFUtil.isRdfOrRdfa(doc);

      c.namespaces.clear();
      triplesToNamespaces(c.pred2space, c.namespaces, doc);
      for (Map.Entry<String, Integer> entry : c.namespaces.entrySet()) {
        c.result.addAll(domain, datasetLabel, url, entry.getKey(),
            entry.getValue(), isRdfOrRdfa);
        functionCall.getOutputCollector().add(c.result);
        c.result.clear();
      }
    } catch (AnalyticsException e) {
      logger.error("Error parsing the json, skipping", e);
    }
  }

  /**
   * Process the list of statements of this document and fills the {@link Map}
   * with the frequency of a namespace.
   */
  private void triplesToNamespaces(final Predicate2Namespace pred2space,
                                   final Map<String, Integer> namespaces,
                                   final RDFDocument doc)
  throws AnalyticsException {
    final List<Triple> triples = doc.getTriples();
    for (Triple t : triples) {
      String uri = null;
      if (AnalyticsClassAttributes.isClass(t.getPredicate().toString())) {
        uri = t.getObject().toString();
        if (t.getObject() instanceof BNode) {
          // we are not interested in black nodes in this case
          continue;
        }
      } else {
        uri = t.getPredicate().toString();
      }
      String namespace = pred2space.getNamespace(uri);

      if (namespace == null) {
        continue;
      }

      if (namespaces.containsKey(namespace)) {
        namespaces.put(namespace, namespaces.get(namespace) + 1);
      } else {
        namespaces.put(namespace, 1);
      }
    }
  }

}
