/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.entity;

import static org.sindice.core.analytics.util.AnalyticsCounters.TIME;
import static org.sindice.graphsummary.cascading.InstanceCounters.BLACK_PREDICATES;
import static org.sindice.graphsummary.cascading.InstanceCounters.EMPTY_VALUE_IN_TRIPLE;
import static org.sindice.graphsummary.cascading.InstanceCounters.INSTANCE_ID;
import static org.sindice.graphsummary.cascading.InstanceCounters.TRIPLES;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import org.apache.hadoop.io.BytesWritable;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.sindice.analytics.entity.AnalyticsBNode;
import org.sindice.analytics.entity.AnalyticsLiteral;
import org.sindice.analytics.entity.AnalyticsUri;
import org.sindice.analytics.entity.AnalyticsValue;
import org.sindice.analytics.entity.BlankNode;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
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
import cascading.tuple.TupleEntryCollector;

/**
 * Parse the JSON input to get the following fields:
 * 
 * [ Domain | Subject-hash | spo ]
 * 
 * Where "Domain" is the hash of the domain, "subject-hash" is the hash of the
 * subject and "spo" is the list of all the triple associated to the subject.
 * 
 * @author Pierre Bailly-Ferry <pierre.bailly@deri.org>
 */
public class StatementFunction
extends SummaryBaseOperation<StatementFunction.Context>
implements Function<StatementFunction.Context> {

  private static final long   serialVersionUID = 1430631649880199105L;
  private static final Logger logger           = LoggerFactory.getLogger(StatementFunction.class);

  class Context {
    final Tuple            tuple     = Tuple.size(5);
    final RDFParser        rdfParser;

    final AnalyticsValue[] subject   = { new AnalyticsUri(), new AnalyticsBNode(), new AnalyticsLiteral() };
    final AnalyticsValue[] predicate = { new AnalyticsUri(), new AnalyticsBNode(), new AnalyticsLiteral() };
    final AnalyticsValue[] object    = { new AnalyticsUri(), new AnalyticsBNode(), new AnalyticsLiteral() };

    public Context(FlowProcess fp) {
      rdfParser = new RDFParser(fp);
    }
  }

  public StatementFunction(final Fields fieldDeclaration) {
    super(fieldDeclaration);
  }

  @Override
  public void prepare(FlowProcess flowProcess,
                      OperationCall<Context> operationCall) {
    super.prepare(flowProcess, operationCall);
    operationCall.setContext(new Context(flowProcess));
  }

  @Override
  public void cleanup(FlowProcess flowProcess, OperationCall<Context> operationCall) {
    operationCall.getContext().rdfParser.clean();
  }

  @Override
  public void operate(FlowProcess flowProcess,
                      FunctionCall<Context> functionCall) {
    final long start = System.currentTimeMillis();

    try {
      final Context c = functionCall.getContext();
      final RDFDocument doc = c.rdfParser.getRDFDocument(functionCall.getArguments());
      final String url = doc.getUrl();
      final long domainHash = Hash.getHash64(doc.getDatasetLabel());

      final String[] triples = doc.getContent();
      flowProcess.increment(INSTANCE_ID, TRIPLES, triples.length);
      c.tuple.set(0, domainHash);
      for (final String triple : triples) {
        final Statement st = c.rdfParser.parseStatement(triple);
        if (st == null) {
          continue;
        }

        // Check the blacklist of predicates
        if (AnalyticsParameters.PREDICATES_BLACKLIST.get().contains(st.getPredicate().stringValue())) {
          flowProcess.increment(INSTANCE_ID, BLACK_PREDICATES, 1);
          continue;
        }
        if (!AnalyticsParameters.PREDICATES_BLACKLIST_REGEXP.get().isEmpty()) {
          boolean skip = false;
          for (String regexp : AnalyticsParameters.PREDICATES_BLACKLIST_REGEXP.get()) {
            if (st.getPredicate().stringValue().matches(regexp)) {
              flowProcess.increment(INSTANCE_ID, BLACK_PREDICATES, 1);
              skip = true;
              break;
            }
          }
          if (skip) {
            continue;
          }
        }

        final AnalyticsValue subject = AnalyticsValue.fromSesame(st.getSubject(), c.subject);
        final AnalyticsValue predicate = AnalyticsValue.fromSesame(st.getPredicate(), c.predicate);
        final AnalyticsValue object = AnalyticsValue.fromSesame(st.getObject(), c.object);
        /*
         * Remove data if:
         * - subject or predicate is empty
         * - the object (not a Literal) is empty
         */
        if (subject.getValue().length == 0 || predicate.getValue().length == 0 ||
            (!(object instanceof Literal) && object.getValue().length == 0)) {
          logger.error("Triple containing empty value, either subject / predicate / object." +
              " Ignoring [{}]", st.toString());
          flowProcess.increment(INSTANCE_ID, EMPTY_VALUE_IN_TRIPLE, 1);
          continue;
        }

        if (!(object instanceof BNode) && !(subject instanceof BNode)) {
          final BytesWritable subHash = Hash.getHash128(subject.getValue());
          collect(functionCall.getOutputCollector(), c.tuple, subHash, subject, predicate, object);
        } else { // We must rename the blank node in order to make it unique
          final long startBnode = System.currentTimeMillis();

          final BytesWritable subHash;
          if (subject instanceof BNode) {
            subHash = BlankNode.encode(url, subject.stringValue());
            subject.setValue(subHash);
          } else {
            subHash = Hash.getHash128(subject.getValue());
          }

          if (object instanceof BNode) {
            final BytesWritable ohash = BlankNode.encode(url, object.stringValue());
            object.setValue(ohash);
          }

          flowProcess.increment(JOB_ID, TIME + this.getClass().getSimpleName() + " - BNode", System.currentTimeMillis() - startBnode);

          collect(functionCall.getOutputCollector(), c.tuple, subHash, subject, predicate, object);
        }
      }
    } catch (AnalyticsException e) {
      logger.error("Error parsing the RDF document, skipping", e);
    }

    flowProcess.increment(JOB_ID, TIME + this.getClass().getSimpleName(), System.currentTimeMillis() - start);
  }

  /**
   * Collect the parsed statement of an entity
   * @param collector of type {@link TupleEntryCollector}
   * @param tuple of type {@link Tuple}
   * @param eHash the hash of the entity as {@link BytesWritable}
   * @param s the subject of the statement as a {@link Value}
   * @param p the predicate of the statement as a {@link Value}
   * @param o the object of the statement as a {@link Value}
   */
  private void collect(final TupleEntryCollector collector,
                       final Tuple tuple,
                       final BytesWritable eHash,
                       final AnalyticsValue s,
                       final AnalyticsValue p,
                       final AnalyticsValue o) {
    tuple.set(1, eHash);
    tuple.set(2, s);
    tuple.set(3, p);
    tuple.set(4, o);
    collector.add(tuple);
  }

}
