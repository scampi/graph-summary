/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.hash2value;

import static org.sindice.core.analytics.util.AnalyticsCounters.ERROR;
import static org.sindice.core.analytics.util.AnalyticsCounters.TIME;
import static org.sindice.graphsummary.cascading.InstanceCounters.INSTANCE_ID;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.sindice.analytics.entity.AnalyticsBNode;
import org.sindice.analytics.entity.AnalyticsLiteral;
import org.sindice.analytics.entity.AnalyticsUri;
import org.sindice.analytics.entity.AnalyticsValue;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
import org.sindice.core.analytics.rdf.AnalyticsClassAttributes;
import org.sindice.core.analytics.rdf.RDFDocument;
import org.sindice.core.analytics.rdf.RDFParser;
import org.sindice.core.analytics.util.AnalyticsException;
import org.sindice.core.analytics.util.Hash;
import org.sindice.graphsummary.cascading.SummaryBaseOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * This {@link Function} outputs a mapping from hash to string for the graph structural data.
 */
public class GenerateDictionaryFunction
extends SummaryBaseOperation<GenerateDictionaryFunction.Context>
implements Function<GenerateDictionaryFunction.Context> {

  private static final long   serialVersionUID = -5283850658508797412L;

  private static final Logger logger           = LoggerFactory.getLogger(GenerateDictionaryFunction.class);

  /**
   * The flag for domain specific {@link Tuple}s
   */
  public static final int     DOMAIN_FLAG      = 0;
  /**
   * The flag for predicate specific {@link Tuple}s
   */
  public static final int     PREDICATE_FLAG   = 1;
  /**
   * The flag for type specific {@link Tuple}s
   */
  public static final int     TYPE_FLAG        = 2;
  /**
   * The flag for datatype specific {@link Tuple}s
   */
  public static final int     DATATYPE_FLAG    = 3;

  class Context {
    final Tuple            tuple     = Tuple.size(3);
    final RDFParser        rdfParser;

    final AnalyticsValue[] predicate = { new AnalyticsUri(), new AnalyticsBNode(), new AnalyticsLiteral() };
    final AnalyticsValue[] object    = { new AnalyticsUri(), new AnalyticsBNode(), new AnalyticsLiteral() };

    final AnalyticsLiteral domain    = new AnalyticsLiteral();

    public Context(FlowProcess fp) {
      rdfParser = new RDFParser(fp);
    }
  }

  public GenerateDictionaryFunction(final Fields fieldDeclaration) {
    super(fieldDeclaration);
  }

  @Override
  public void prepare(final FlowProcess flowProcess,
                      final OperationCall<Context> operationCall) {
    super.prepare(flowProcess, operationCall);
    operationCall.setContext(new Context(flowProcess));
  }

  @Override
  public void operate(FlowProcess flowProcess,
                      FunctionCall<Context> functionCall) {
    final long start = System.currentTimeMillis();

    final Context c = functionCall.getContext();

    try {
      final RDFDocument doc = c.rdfParser.getRDFDocument(functionCall.getArguments());
      final String datasetLabel = doc.getDatasetLabel();

      final String[] triples = doc.getContent();

      /*
       * domain dictionary
       */
      c.domain.setValue(datasetLabel);
      c.tuple.set(0, DOMAIN_FLAG);
      c.tuple.set(1, Hash.getHash64(datasetLabel));
      c.tuple.set(2, c.domain);
      functionCall.getOutputCollector().add(c.tuple);

      for (final String triple : triples) {
        final Statement st = c.rdfParser.parseStatement(triple);
        if (st == null) {
          logger.error("Error parsing the triple for the string {}, skipping: ", triple);
          flowProcess.increment(INSTANCE_ID, ERROR + "RDF_PARSE", 1);
          continue;
        }

        final AnalyticsValue predicate = AnalyticsValue.fromSesame(st.getPredicate(), c.predicate);
        final AnalyticsValue object = AnalyticsValue.fromSesame(st.getObject(), c.object);

        // If predicate is missing, remove
        if (predicate.getValue().length == 0) {
          logger.error("Triple containing empty predicate. Ignoring [{}]", st);
          continue;
        }
        final boolean isType = AnalyticsClassAttributes.isClass(predicate);
        // If type is a literal, normalise
        if (isType && AnalyticsParameters.NORM_LITERAL_TYPE.get() && st.getObject() instanceof Literal) {
          final String oString = AnalyticsClassAttributes.normalizeLiteralTypeLabel(object.stringValue());
          if (oString == null) {
            logger.error("Couldn't export the class from the Literal type. Ignoring [{}]", st);
            continue;
          }
          object.setValue(oString);
        }

        if (!isType) {
          /*
           * predicate dictionary
           */
          c.tuple.set(0, PREDICATE_FLAG);
          c.tuple.set(1, Hash.getHash64(predicate.getValue()));
          c.tuple.set(2, predicate);
          functionCall.getOutputCollector().add(c.tuple);
          /*
           * Datatype dictionary
           */
          if (object instanceof AnalyticsLiteral) {
            final AnalyticsUri datatype = ((AnalyticsLiteral) object).getDatatypeAsAnalyticsURI();
            if (datatype.getValue().length !=0) {
              c.tuple.set(0, DATATYPE_FLAG);
              c.tuple.set(1, Hash.getHash64(datatype.getValue()));
              c.tuple.set(2, datatype);
              functionCall.getOutputCollector().add(c.tuple);
            }
          }
        } else {
          /*
           * type dictionary
           */
          c.tuple.set(0, TYPE_FLAG);
          c.tuple.set(1, Hash.getHash64(object.getValue()));
          c.tuple.set(2, object);
          functionCall.getOutputCollector().add(c.tuple);
        }
      }
    } catch (AnalyticsException e) {
      logger.error("", e);
    }

    flowProcess.increment(JOB_ID, TIME + this.getClass().getSimpleName(), System.currentTimeMillis() - start);
  }

}
