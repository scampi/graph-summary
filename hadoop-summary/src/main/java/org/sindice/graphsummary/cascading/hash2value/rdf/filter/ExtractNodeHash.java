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
 * This {@link Function} extracts the identifier of a node of the graph summary.
 * This extraction relies on the URI scheme used in the graph summary.
 * 
 * @see RDFPropertiesGraphFunction
 */
public class ExtractNodeHash
extends BaseOperation<Tuple>
implements Function<Tuple> {

  private static final long serialVersionUID = -3624181351650897569L;

  public ExtractNodeHash(final Fields fields) {
    super(1, fields);
  }

  @Override
  public void prepare(FlowProcess flowProcess,
                      OperationCall<Tuple> operationCall) {
    operationCall.setContext(Tuple.size(2));
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Tuple> functionCall) {
    final TupleEntry args = functionCall.getArguments();
    final String quad = args.getString("quad");

    final String subUri = getSubjectUri(quad);
    final String h = subUri.replace(NodeFilterSummaryGraph.NODE_PREFIX, "") // rm node prefix
                           .replaceFirst("/.*", ""); // rm any sub nodes
    final long nodeHash = Hash.getHash64(h);

    final Tuple c = functionCall.getContext();
    c.set(0, nodeHash);
    c.set(1, quad);
    functionCall.getOutputCollector().add(c);
  }

  /**
   * Returns the Subject URI of the given quad.
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

}
