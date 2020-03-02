/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.stats.assembly;

import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.mapred.JobConf;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
import org.sindice.core.analytics.rdf.AnalyticsClassAttributes;
import org.sindice.core.analytics.rdf.RDFDocument;
import org.sindice.core.analytics.rdf.RDFParser;
import org.sindice.core.analytics.rdf.Triple;
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
public class Json2ClassPredicateFunction
extends BaseOperation<Json2ClassPredicateFunction.Context>
implements Function<Json2ClassPredicateFunction.Context> {

  private static final long   serialVersionUID = 1L;

  private static final Logger logger           = LoggerFactory.getLogger(Json2ClassPredicateFunction.class);

  public Json2ClassPredicateFunction(Fields fieldDeclaration) {
    super(fieldDeclaration);
  }

  public Json2ClassPredicateFunction() {
    this(new Fields("sndDomain", "url", "predicate", "class", "objectsNumber"));
  }

  public static class Context {
    final Tuple                                     result = new Tuple();
    final HashMap<String, HashMap<String, Integer>> types  = new HashMap<String, HashMap<String, Integer>>();
    final RDFParser                                 rdfParser;

    public Context(FlowProcess fp) {
      rdfParser = new RDFParser(fp);
    }
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall) {
    final JobConf conf = ((HadoopFlowProcess) flowProcess).getJobConf();

    // Class Attributes
    final String[] classAttributes = conf
    .getStrings(AnalyticsParameters.CLASS_ATTRIBUTES_FIELD.toString(), AnalyticsParameters.CLASS_ATTRIBUTES_FIELD.get());
    AnalyticsClassAttributes.initClassAttributes(classAttributes);
    // Normalise literal types
    final String norm = flowProcess.getStringProperty(AnalyticsParameters.NORM_LITERAL_TYPE.toString());
    AnalyticsParameters.NORM_LITERAL_TYPE.set(Boolean.parseBoolean(norm));

    operationCall.setContext(new Context(flowProcess));
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Context> functionCall) {
    final Context context = functionCall.getContext();

    context.types.clear();
    context.result.clear();

    try {
      final RDFDocument doc = context.rdfParser.getRDFDocument(functionCall.getArguments());
      final String url = doc.getUrl();
      final String datasetLabel = doc.getDatasetLabel();

      final List<Triple> triples = doc.getTriples();
      for (Triple s : triples) {

        // Only predicates of type
        final String predicate = s.getPredicateString();
        if (AnalyticsClassAttributes.isClass(predicate)) {

          // Normalize Object
          final Value object = s.getObject();
          if (!(object instanceof BNode)) {

            final String oString;
            if (AnalyticsParameters.NORM_LITERAL_TYPE.get() && object instanceof Literal) {
              oString = AnalyticsClassAttributes.normalizeLiteralType(s.getObjectString());
            } else {
              oString = s.getObjectString();
            }

            // Key list of types
            if (!context.types.containsKey(predicate)) {
              context.types.put(predicate, new HashMap<String, Integer>());
            }

            if (!context.types.get(predicate).containsKey(oString)) {
              context.types.get(predicate).put(oString, 1);
            } else {
              context.types.get(predicate).put(oString, context.types.get(predicate).get(oString) + 1);
            }
          }
        }
      }

      for (Entry<String, HashMap<String, Integer>> entry : context.types.entrySet()) {
        final String predicate = entry.getKey();
        for (Entry<String, Integer> entry2 : entry.getValue().entrySet()) {
          final String object = entry2.getKey();
          final int count = entry2.getValue();

          context.result.addAll(datasetLabel, url, predicate, object, count);
          functionCall.getOutputCollector().add(context.result);
          context.result.clear();
        }
      }
    } catch (AnalyticsException e) {
      logger.error("Error parsing the json, skipping", e);
    }
  }

}
