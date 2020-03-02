/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.stats.assembly;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
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
public class Json2ClassFunction
extends BaseOperation<Json2ClassFunction.Context>
implements Function<Json2ClassFunction.Context> {

  private static final long    serialVersionUID = 1L;

  private static final Logger  logger           = LoggerFactory.getLogger(Json2ClassFunction.class);

  public static final String[] OUTPUT_FIELDS = new String[] { "domain",
      "sndDomain", "url", "class", "objectsNumber", "isRdfOrRdfa" };

  public Json2ClassFunction(Fields fieldDeclaration) {
    super(fieldDeclaration);
  }

  public Json2ClassFunction() {
    this(new Fields(OUTPUT_FIELDS));
  }

  public static class Context {
    final Tuple                result  = new Tuple();
    final Map<String, Integer> classes = new HashMap<String, Integer>();
    final RDFParser            rdfParser;

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

    try {
      final RDFDocument doc = context.rdfParser.getRDFDocument(functionCall.getArguments());
      final String domain = doc.getDomain();
      final String url = doc.getUrl();
      final String datasetLabel = doc.getDatasetLabel();

      final boolean isRdfOrRdfa = RDFUtil.isRdfOrRdfa(doc);

      context.classes.clear();
      triplesToClasses(flowProcess, context.classes, doc);

      for (Map.Entry<String, Integer> entry : context.classes.entrySet()) {
        context.result.addAll(domain, datasetLabel, url, entry.getKey(), entry.getValue(), isRdfOrRdfa);
        functionCall.getOutputCollector().add(context.result);
        context.result.clear();
      }
    } catch (Exception e) {
      logger.error("Error parsing the json, skipping", e);
    }
  }

  /**
   * Process the statements of this document and fills the {@link Map} with a list of class frequency. A class is an
   * object which predicate is a class attribute, i.e., the method {@link AnalyticsClassAttributes#isClass(String)} is
   * returns true.
   */
  protected void triplesToClasses(final FlowProcess<?> flowProcess,
                                  final Map<String, Integer> classes,
                                  final RDFDocument doc)
  throws AnalyticsException {
    final List<Triple> triples = doc.getTriples();
    for (Triple s : triples) {
      if (AnalyticsClassAttributes.isClass(s.getPredicateString())) {
        String clazz = s.getObjectString();

        if (s.getObject() instanceof BNode) {
          // we are not interested in black node classes
          continue;
        } else if (AnalyticsParameters.NORM_LITERAL_TYPE.get() && s.getObject() instanceof Literal) {
          clazz = AnalyticsClassAttributes.normalizeLiteralType(clazz);
        }

        if (clazz != null) {
          if (classes.containsKey(clazz)) {
            classes.put(clazz, classes.get(clazz) + 1);
          } else {
            classes.put(clazz, 1);
          }
        }
      }
    }
  }

}
