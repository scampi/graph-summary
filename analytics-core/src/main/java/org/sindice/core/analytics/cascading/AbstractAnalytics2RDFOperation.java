/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading;

import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.rio.ntriples.NTriplesUtil;
import org.openrdf.sail.memory.model.MemValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;


/**
 * Base class for output RDF data.
 */
public abstract class AbstractAnalytics2RDFOperation<C extends AbstractAnalytics2RDFOperation.Context>
extends BaseOperation<C> {

  private static final long serialVersionUID = 9038825202589427455L;

  public static final Logger logger = LoggerFactory.getLogger(AbstractAnalytics2RDFOperation.class);

  public class Context {
    public final StringBuilder sb    = new StringBuilder();
    public final Tuple         res   = new Tuple();
    public final ValueFactory  value = new MemValueFactory();
  }

  public AbstractAnalytics2RDFOperation(int numArgs, Fields fieldDeclaration) {
    super(numArgs, fieldDeclaration);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<C> operationCall) {
    operationCall.setContext(getContext(flowProcess));
  }

  /**
   * Returns an instance of a custom Context class.
   */
  protected abstract C getContext(FlowProcess<?> flowProcess);

  /**
   * Write a triple and add it to the tuple collector
   */
  protected void writeStatement(final C cxt,
                                TupleEntryCollector collector,
                                final Value s,
                                final Value p,
                                final Value o) {
    final String ntriple = writeStatement(cxt, s, p, o);
    cxt.res.clear();
    cxt.res.add(ntriple);
    collector.add(cxt.res);
  }

  /**
   * Write a triple
   * @return A String representing the statement in NTriple format
   */
  protected final String writeStatement(final C cxt,
                                        final Value s,
                                        final Value p,
                                        final Value o) {
    cxt.sb.setLength(0);
    cxt.sb.append(NTriplesUtil.toNTriplesString(s)).append(' ')
        .append(NTriplesUtil.toNTriplesString(p)).append(' ')
        .append(NTriplesUtil.toNTriplesString(o)).append(" .");
    return cxt.sb.toString();
  }


  /**
   * Write a quad and add it to the collector
   */
  protected void writeStatement(final C cxt,
                                TupleEntryCollector collector,
                                final Value s,
                                final Value p,
                                final Value o,
                                final Value c) {
    final String quad = writeStatement(cxt, s, p, o, c);
    cxt.res.clear();
    cxt.res.add(quad);
    collector.add(cxt.res);
  }

  /**
   * Write a quad
   */
  protected final String writeStatement(final C cxt,
                                        final Value s,
                                        final Value p,
                                        final Value o,
                                        final Value c) {
    cxt.sb.setLength(0);
    cxt.sb.append(NTriplesUtil.toNTriplesString(s)).append(' ')
        .append(NTriplesUtil.toNTriplesString(p)).append(' ')
        .append(NTriplesUtil.toNTriplesString(o)).append(' ')
        .append(NTriplesUtil.toNTriplesString(c)).append(" .");
    return cxt.sb.toString();
  }

}
