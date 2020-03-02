package org.sindice.core.analytics.stats.domain;

import java.util.List;

import org.sindice.core.analytics.rdf.RDFDocument;
import org.sindice.core.analytics.rdf.RDFParser;
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

public class CountTriplesJsonFunction
extends BaseOperation<CountTriplesJsonFunction.Context>
implements Function<CountTriplesJsonFunction.Context> {

  private static final long   serialVersionUID = 1L;
  private static final Logger logger           = LoggerFactory.getLogger(CountTriplesJsonFunction.class);

  class Context {
    final Tuple     tuple = new Tuple();
    final RDFParser rdfParser;

    public Context(FlowProcess fp) {
      rdfParser = new RDFParser(fp);
    }
  }

  public CountTriplesJsonFunction(Fields fieldDeclaration) {
    super(fieldDeclaration);
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
      final String datasetLabel = doc.getDatasetLabel();
      final long sndDomainHash = Hash.getHash64(datasetLabel);

      c.tuple.clear();
      c.tuple.addAll(sndDomainHash, datasetLabel, getNbTriples(doc, RDFDocument.EXPLICIT_CONTENT),
        getNbTriples(doc, RDFDocument.IMPLICIT_CONTENT));
      functionCall.getOutputCollector().add(c.tuple);
    } catch (Exception e) {
      logger.error("Error parsing the RDF document, skipping", e);
    }
  }

  private long getNbTriples(final RDFDocument doc, final String field) {
    final Object contentObj = doc.get(field);

    if (contentObj == null) {
      return 0;
    }
    if (contentObj instanceof String) {
      return 1;
    } else {
      return ((List<String>) contentObj).size();
    }
  }

}
