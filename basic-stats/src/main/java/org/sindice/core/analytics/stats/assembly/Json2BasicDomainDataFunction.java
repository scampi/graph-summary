/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.stats.assembly;

import java.util.List;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.sindice.core.analytics.rdf.RDFDocument;
import org.sindice.core.analytics.rdf.RDFParser;
import org.sindice.core.analytics.rdf.Triple;
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
 * Parse function to get fields codified in json, produce data for simple stats on domains: -sndDomain -domain -url
 * -#triples -#blanknodes -#uri as object -#literals
 */
public class Json2BasicDomainDataFunction
extends BaseOperation<Json2BasicDomainDataFunction.Context>
implements Function<Json2BasicDomainDataFunction.Context> {

  private static final long    serialVersionUID = 1L;

  private static final Logger  logger           = LoggerFactory.getLogger(Json2BasicDomainDataFunction.class);

  public boolean               filterBlankNodes = false;

  public static final String[] OUTPUT_FIELDS    = new String[] { "sndDomain", "domain", "url", "nTriple", "nBNode",
      "nUri", "nLiteral"                       };

  public static class Context {
    final Tuple     result = new Tuple();
    Integer         nBNode;
    Integer         nUri;
    Integer         nLiteral;
    final RDFParser rdfParser;

    public Context(FlowProcess fp) {
      rdfParser = new RDFParser(fp);
    }
  }

  public Json2BasicDomainDataFunction(Fields fieldDeclaration) {
    super(fieldDeclaration);
  }

  public Json2BasicDomainDataFunction() {
    this(new Fields(OUTPUT_FIELDS));
  }

  public Json2BasicDomainDataFunction(boolean filterBlankNodes) {
    this();
    this.filterBlankNodes = filterBlankNodes;
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall) {
    operationCall.setContext(new Context(flowProcess));
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Context> functionCall) {
    final Context context = functionCall.getContext();

    context.nBNode = 0;
    context.nUri = 0;
    context.nLiteral = 0;
    try {
      final RDFDocument doc = context.rdfParser.getRDFDocument(functionCall.getArguments());
      final String domain = doc.getDomain();
      final String url = doc.getUrl();
      final String datasetLabel = doc.getDatasetLabel();

      final List<Triple> triples = doc.getTriples();
      for (Triple s : triples) {

        // Filter statements with blank nodes
        if (filterBlankNodes) {
          if ((s.getObject() instanceof BNode) || (s.getSubject() instanceof BNode)) {
            logger.debug("Ignoring statement {} (contains blank nodes)", s.toString());
            continue;
          }
        }
        if (s.getObject() instanceof Literal) {
          context.nLiteral++;
        }
        if (s.getObject() instanceof URI) {
          context.nUri++;
        }
        if (s.getObject() instanceof BNode) {
          context.nBNode++;
        }
      }
      context.result.clear();
      context.result.addAll(datasetLabel, domain, url, triples.size(), context.nBNode, context.nUri, context.nLiteral);
      functionCall.getOutputCollector().add(context.result);
    } catch (Exception e) {
      logger.error("Error parsing the json, skipping", e);
    }
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof Json2BasicDomainDataFunction)) {
      return false;
    }
    if (!super.equals(object)) {
      return false;
    }

    final Json2BasicDomainDataFunction func = (Json2BasicDomainDataFunction) object;
    return func.filterBlankNodes == filterBlankNodes;
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = hash * 31 + (filterBlankNodes ? 0 : 1);
    return hash;
  }

}
