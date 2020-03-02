/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.hash2value.rdf.filter;

import org.sindice.core.analytics.util.Hash;
import org.sindice.graphsummary.cascading.hash2value.rdf.RDFPropertiesGraphFunction;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * This class is used for processing the edge of a Summary graph.
 *
 * The extraction process relies on the Summary scheme {@link RDFPropertiesGraphFunction}.
 * 
 * <p>
 * 
 * This function extracts into the first field the source, and in the second field the target of the edge.
 * The third field is the incoming quad.
 */
public class ExtractEdgeSourceTargetHash
extends BaseOperation<Tuple>
implements Function<Tuple> {

  private static final long serialVersionUID = 3137252923646522071L;

  public ExtractEdgeSourceTargetHash(final Fields fields) {
    super(1, fields);
  }

  @Override
  public void prepare(FlowProcess flowProcess,
                      OperationCall<Tuple> operationCall) {
    operationCall.setContext(Tuple.size(3));
  }

  @Override
  public void operate(FlowProcess flowProcess,
                      FunctionCall<Tuple> functionCall) {
    final TupleEntry args = functionCall.getArguments();
    final String quad = args.getString("quad");

    final String subUri = getSubjectUri(quad);

    final long targetHash = getEdgeTarget(subUri);
    final long sourceHash = getEdgeSource(subUri);

    final Tuple c = functionCall.getContext();
    c.set(0, sourceHash);
    c.set(1, targetHash);
    c.set(2, quad);
    functionCall.getOutputCollector().add(c);
  }

  /**
   * Return the subject URI
   */
  private String getSubjectUri(final String quad) {
    final int endInd = quad.indexOf('>');
    if (endInd == -1) {
      throw new RuntimeException("Unable to find character '>' in: " + quad);
    }
    final int startInd = quad.indexOf('<');
    if (startInd == -1) {
      throw new RuntimeException("Unable to find character '>' in: " + quad);
    }
    return quad.substring(startInd + 1, endInd);
  }

  /**
   * Return the source identifier of the edge.
   * @param uri the edge URI
   */
  private long getEdgeSource(final String uri) {
    final int ind = uri.indexOf("/source") + 7;
    final int lastSlash = uri.lastIndexOf('/');
    if (ind == -1 || lastSlash == -1) {
      throw new RuntimeException("malformated edge URI: " + uri);
    }
    return Hash.getHash64(uri.substring(ind, lastSlash));
  }

  /**
   * Return the target identifier of the edge.
   * @param uri the edge URI
   */
  private long getEdgeTarget(final String uri) {
    final int ind = uri.indexOf("/target") + 7;
    if (ind == -1) {
      throw new RuntimeException("malformated edge URI: " + uri);
    }
    return Hash.getHash64(uri.substring(ind));
  }

}
