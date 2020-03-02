/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster.fbisimulation.splitting;

import static org.sindice.core.analytics.util.AnalyticsCounters.TIME;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.io.BytesWritable;
import org.sindice.graphsummary.cascading.ecluster.fbisimulation.UpdateCidFunction;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.hadoop.collect.HadoopTupleCollectionFactory;
import cascading.tuple.io.TupleOutputStream;

/**
 * This {@link Buffer} class splits unstable clusters.
 * <p>
 * If the entities within a cluster do not reach the same set of outgoing clusters, the cluster is then unstable and
 * will be split. The fields in first position of the output tuple is used as a flag, equal to <code>null</code> when
 * the cluster is split. To perform the splitting, the cluster identifier of each entity is updated with the set of
 * clusters identifier this entity reaches, using the function {@link UpdateCidFunction}.
 */
@SuppressWarnings("rawtypes")
public class SplitBuffer
extends BaseOperation<HadoopTupleCollectionFactory>
implements Buffer<HadoopTupleCollectionFactory> {

  private static final long  serialVersionUID = -604023278969945347L;
  /** This is counter that indicates how many clusters were split */
  public static final String SPLITS           = "splits";

  public SplitBuffer(final Fields decFields) {
    super(decFields);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<HadoopTupleCollectionFactory> operationCall) {
    HadoopTupleCollectionFactory f = new HadoopTupleCollectionFactory();
    f.initialize(flowProcess);
    operationCall.setContext(f);
  }

  @Override
  public void operate(FlowProcess flowProcess, BufferCall<HadoopTupleCollectionFactory> bufferCall) {
    final long start = System.currentTimeMillis();

    // Do not use a Context object, because a Buffer must be reentrant
    final Collection<Tuple> entities;
    entities = bufferCall.getContext().create(flowProcess);

    final TupleEntryCollector out = bufferCall.getOutputCollector();
    final Iterator<TupleEntry> it = bufferCall.getArgumentsIterator();
    final Object domain = bufferCall.getGroup().getObject("domain");
    final BytesWritable cid = (BytesWritable) bufferCall.getGroup().getObject("cid");
    final Tuple tuple = Tuple.size(6); // used only if the cluster is split

    boolean isSplit = false; // if true, this cluster is split
    boolean isStable = false; // if true, this cluster is stable
    // Here we use ObjectUtils.NULL to differentiate when the "cid-right" field is
    // really null.
    Object prevCidRight = ObjectUtils.NULL; // this is used to know if there is a split.

    // "domain", "entity", "incoming", "cid", "entity-right", "cid-right"
    // "flag", "domain", "entity", "incoming", "cid"
    while (it.hasNext()) {
      final TupleEntry te = it.next();
      final BytesWritable entity = (BytesWritable) te.getObject("entity"); // never null
      final Object incoming = te.getObject("incoming");
      // if cid-right is null, it means that the outgoing entity(ies) is not in the splits tap.
      // This is the only case, since all the sink entities have been filtered out.
      final Tuple cidRight = (Tuple) te.getObject("cid-right");

      if (isSplit) { // This cluster has been split
        collectEntity(tuple, out, true, domain, entity, incoming, cid, cidRight);
      } else if (isStable || (entities.isEmpty() && cidRight == null)) {
        // because the tuples are ordered by cid-right, if the first cid-right we get is null,
        // then it means all are null. There was no entity to which this cluster points to in the current split set.
        isStable = true;
        collectEntity(tuple, out, false, domain, entity, incoming, cid, null);
      } else if (prevCidRight != ObjectUtils.NULL && !cidEqual(cidRight, (Tuple) prevCidRight)) { // Split detected.
        // first split
        // Flush the entities list. They are mapped to a different cluster.
        for (final Tuple t : entities) {
          t.set(0, null); // Put flag to null, to indicate this cluster is split.
          t.set(5, prevCidRight);
          out.add(t);
        }
        // Collect the current entity. This is the first entity that goes into a different split.
        collectEntity(tuple, out, true, domain, entity, incoming, cid, cidRight);
        isSplit = true;
        entities.clear();
      } else {
        // still no split
        entities.add(new Tuple(0, domain, entity, incoming, cid, null));
        prevCidRight = cidRight;
      }
    }

    // if entities contains tuples, then it means there has been no split.
    for (final Tuple t : entities) {
	  // the last field (cid-right) is null, so that the cluster identifier is not updated in UpdateCidFunction
      out.add(t);
    }
    entities.clear();

    if (isSplit && isStable) {
      throw new RuntimeException("The current cluster [" + cid + "] is split and stable at the same time!");
    }
    flowProcess.increment(JOB_ID, "stables", isStable ? 1 : 0);
    flowProcess.increment(JOB_ID, SPLITS, isSplit ? 1 : 0);

    flowProcess.increment(JOB_ID, TIME + this.getClass().getSimpleName(), System.currentTimeMillis() - start);
  }

  /**
   * Returns <code>true</code> if both {@link Tuple}s are equals or <code>null</code>.
   * @param a a {@link Tuple}
   * @param b a {@link Tuple}
   * @return <code>true</code> if <code>a == b</code>
   */
  private boolean cidEqual(Tuple a, Tuple b) {
    if (a == null && b == null) {
      return true;
    }
    if ((a == null && b != null) || (a != null && b == null)) {
      return false;
    }
    return a.equals(b);
  }

  /**
   * Collect the given entity, setting the flag to <code>null</code> if isSplit is <code>true</code>, to
   * <code>0</code> otherwise.
   * @param tuple the {@link Tuple} that will be collected. It must initialised with 5 fields.
   * @param out the {@link TupleOutputStream} of the operation
   * @param isSplit a boolean flag indicating if this cluster is split
   * @param domain the domain of the document where this entity was found
   * @param entity the entity identifier
   * @param incoming the incoming set of the entity
   * @param cid the cluster identifier this entity belongs to
   * @param cidRight the set of outgoing clusters identifier that this entity reaches
   */
  private void collectEntity(final Tuple tuple,
                             final TupleEntryCollector out,
                             final boolean isSplit,
                             final Object domain,
                             final BytesWritable entity,
                             final Object incoming,
                             final Object cid,
                             final Tuple cidRight) {
    tuple.set(0, isSplit ? null : 0);
    tuple.set(1, domain);
    tuple.set(2, entity);
    tuple.set(3, incoming);
    tuple.set(4, cid);
    tuple.set(5, cidRight);
    out.add(tuple);
  }

}
