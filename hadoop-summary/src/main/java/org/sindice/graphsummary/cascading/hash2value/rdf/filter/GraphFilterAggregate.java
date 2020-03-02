/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.hash2value.rdf.filter;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;

import org.openrdf.model.Statement;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.rio.ntriples.NTriplesUtil;
import org.openrdf.sail.memory.MemoryStore;
import org.sindice.core.analytics.util.AnalyticsUtil;
import org.sindice.core.analytics.util.Hash;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

/**
 * This {@link Aggregator} evaluates the SPARQL query against the node's triples.
 * If a node matches the query, it is flagged for removal using the field named "flag".
 * If that field is null, the node will be filtered out.
 */
public class GraphFilterAggregate
extends BaseOperation<GraphFilterAggregate.Context>
implements Aggregator<GraphFilterAggregate.Context> {

  private static final long serialVersionUID = -917786515807932744L;

  /** The WHERE clause of the SPARQL query that matches nodes to filter */
  private final String filterQuery;

  class Context {
    final RepositoryConnection con;
    final BooleanQuery askQuery;
    final CollectorHandler rdfHandler = new CollectorHandler();

    /**
     * This {@link RDFHandlerBase} outputs the RDF data of nodes that don't match the query.
     * If they do match, it outputs the node ID. In order to differentiate between the two outputed tuples, the first
     * field is used as a flag, i.e., it is null if the node matched the query.
     */
    class CollectorHandler extends RDFHandlerBase {

      private final StringBuilder sb = new StringBuilder();
      private TupleEntryCollector collector;
      private FlowProcess flowProcess;

      private final Tuple tuple = Tuple.size(2);
      private boolean keep;
      private long nodeHash;

      /**
       * Used to output the {@link Tuple}s
       */
      public void setCollector(TupleEntryCollector collector) {
        this.collector = collector;
      }

      /**
       * @param flowProcess the flowProcess to set
       */
      public void setFlowProcess(FlowProcess flowProcess) {
        this.flowProcess = flowProcess;
      }

      /**
       * <code>true</code> if the node should be kept.
       */
      public void setKeep(boolean keep) {
        this.keep = keep;
      }

      @Override
      public void startRDF()
      throws RDFHandlerException {
      }

      @Override
      public void handleStatement(Statement st)
      throws RDFHandlerException {
        if (keep) {
          sb.setLength(0);
          sb.append(NTriplesUtil.toNTriplesString(st.getSubject())).append(' ')
            .append(NTriplesUtil.toNTriplesString(st.getPredicate())).append(' ')
            .append(NTriplesUtil.toNTriplesString(st.getObject())).append(" .");
          tuple.set(0, 0); // if non null, will keep it
          tuple.set(1, sb.toString());
          collector.add(tuple);
        } else {
          // This node is to be removed. The node identifier needs to be
          // extracted so that connected edges are also removed.
          final String subUri = st.getSubject().stringValue();
          final String h = subUri.replace(NodeFilterSummaryGraph.NODE_PREFIX, "") // rm node prefix
                                 .replaceFirst("/.*", ""); // rm any sub nodes
          nodeHash = Hash.getHash64(h);
        }
      }

      @Override
      public void endRDF()
      throws RDFHandlerException {
        if (!keep) {
          flowProcess.increment(GraphFilterAggregateBy.class.getSimpleName(), GraphFilterAggregateBy.FILTERED, 1);
          tuple.set(0, null); // if non null, will keep it
          tuple.set(1, nodeHash);
          collector.add(tuple);
        }
      }

    }

    public Context() {
      // Create the in-memory repository
      try {
        final MemoryStore memStore = new MemoryStore();
        memStore.setPersist(false);
        final SailRepository repo = new SailRepository(memStore);
        repo.initialize();
        con = repo.getConnection();
      } catch(Exception e) {
        throw new RuntimeException(e);
      }
      // Create the filter query
      try {
        final String ask = "ASK { " + filterQuery + " }";
        askQuery = con.prepareBooleanQuery(QueryLanguage.SPARQL, ask);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public GraphFilterAggregate(final Fields fields,
                              final String filterQuery) {
    super(1, fields);
    this.filterQuery = filterQuery;
  }

  @Override
  public void prepare(final FlowProcess flowProcess,
                      final OperationCall<Context> operationCall) {
    operationCall.setContext(new Context());
  }

  @Override
  public void start(final FlowProcess flowProcess,
                    final AggregatorCall<Context> aggregatorCall) {
    final Context c = aggregatorCall.getContext();

    try {
      c.con.clear();
    } catch (RepositoryException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Aggregate the node's triples.
   */
  @Override
  public void aggregate(FlowProcess flowProcess,
                        AggregatorCall<Context> aggregatorCall) {
    final Context c = aggregatorCall.getContext();
    final byte[] quads = (byte[]) aggregatorCall.getArguments().getObject("quad");
    try {
      c.con.add(new ByteArrayInputStream(quads), "", RDFFormat.NTRIPLES);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Evaluate the query against the node
   */
  @Override
  public void complete(FlowProcess flowProcess,
                       AggregatorCall<Context> aggregatorCall) {
    final Context c = aggregatorCall.getContext();

    try {
      c.con.commit();

      final long size = c.con.size();
      final String name = GraphFilterAggregateBy.class.getSimpleName();
      AnalyticsUtil.incrementByRange(flowProcess, name, GraphFilterAggregateBy.NB_TRIPLES, size,
        50, 100, 500, 1000, 5000, 10000);

      final StringWriter w = new StringWriter();
      c.rdfHandler.setCollector(aggregatorCall.getOutputCollector());
      c.rdfHandler.setFlowProcess(flowProcess);
      // keep node if it doesn't match the query.
      c.rdfHandler.setKeep(!c.askQuery.evaluate());
      c.con.export(c.rdfHandler);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Shuts down the in-memory SPARQL endpoint
   */
  @Override
  public void cleanup(final FlowProcess flowProcess,
                      final OperationCall<Context> operationCall) {
    final RepositoryConnection con = operationCall.getContext().con;

    try {
      if (con != null) {
        con.close();
      }
    } catch (RepositoryException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        con.getRepository().shutDown();
      } catch (RepositoryException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof GraphFilterAggregate)) {
      return false;
    }
    if (!super.equals(object)) {
      return false;
    }

    final GraphFilterAggregate graph = (GraphFilterAggregate) object;
    return graph.filterQuery.equals(filterQuery);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = hash * 31 + filterQuery.hashCode();
    return hash;
  }

}
