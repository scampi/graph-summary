/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.relationships;

import static org.sindice.core.analytics.util.AnalyticsCounters.TUPLES_READ;
import static org.sindice.core.analytics.util.AnalyticsCounters.TUPLES_WRITTEN;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;
import static org.sindice.graphsummary.cascading.JobCounters.JOIN1;
import static org.sindice.graphsummary.cascading.JobCounters.JOIN2;
import static org.sindice.graphsummary.cascading.SummaryCounters.RELATIONS;
import static org.sindice.graphsummary.cascading.SummaryCounters.SUMMARY_ID;

import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.sindice.graphsummary.cascading.ecluster.ClusterSubAssembly;
import org.sindice.graphsummary.cascading.entity.GetClusterGraph;

import cascading.operation.Identity;
import cascading.operation.state.Counter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.CountBy;
import cascading.tuple.Fields;

/**
 * Flow to get the relations from the output of ClusterGeneratorGraph.
 * 
 * Two steps: First parse the <spo> with SPO2HashInterclassFunction, then Join
 * with the table Node-hash/CID to get the CID associated to any node in
 * RelationGraph2JOINS
 * 
 * Input:
 * 
 * [ domain | subject-hash | spo | cluster-id ]
 * 
 * Output:
 * 
 * [ doc-domain | subject-domain | subject-cid | predicate-hash | object-domain
 * | object-cid | total ]
 * 
 * @author Pierre Bailly-Ferry <pierre.bailly@deri.org>
 */
@AnalyticsName(value="relations-graph")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(from=GetClusterGraph.class),
  @AnalyticsPipe(from=ClusterSubAssembly.class)
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "doc-domain", "subject-domain", "subject-cid", "predicate-hash", "object-domain", "object-cid", "total" })
})
public class RelationsGraph extends AnalyticsSubAssembly {

  private static final long  serialVersionUID  = 5938947066367398634L;

  public RelationsGraph() {
    super();
  }

  public RelationsGraph(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    // Read the input and parse the spo format to extract the relations
    Pipe relations = new Each(new Pipe("relations", previous[0]),
      new Counter(JOB_ID, TUPLES_READ + name));
    relations = new Each(relations,
        new SPO2HashInterclassFunction(new Fields("domain", "subject-hash", "predicate-hash", "object-hash")));

    // Read output from the typePipe, the entity-class table
    Pipe types = new Pipe("table-entity/cluster", previous[1]);
    types = new Each(types, new Fields("domain", "subject-hash", "cluster-id"), new Identity());
    types = new Each(types, new Counter(JOB_ID, TUPLES_READ + name));

    // NOW merge the entity-class table with relationships.

    // JOIN on the object hash of the relation
    Fields lftFields = new Fields("object-hash");
    final Fields rhtFields = new Fields("subject-hash");
    final Fields outputFields = new Fields("domain", "subject-hash", "predicate-hash", "object-hash", "d-node-hash", "node-hash", "cluster-id");
    Pipe join1 = new CoGroup("Object-Join", relations, lftFields, types, rhtFields, outputFields);
    join1 = new Each(join1, new Fields("domain", "subject-hash", "predicate-hash", "d-node-hash", "cluster-id"),
      new Identity(new Fields("domain", "subject-hash", "predicate-hash", "object-domain", "object-cid")));
    join1 = new Each(join1, new Counter(JOB_ID, JOIN1));

    // JOIN on the subject hash of the relation
    Fields lftFields1 = new Fields("subject-hash");
    final Fields rhtFields1 = new Fields("subject-hash");
    final Fields outputFields1 = new Fields("domain", "subject-hash", "predicate-hash", "object-domain", "object-cid",
      "d-node-hash", "node-hash", "cluster-id");
    Pipe join2 = new CoGroup("Subject-Join", join1, lftFields1, types, rhtFields1, outputFields1);
    join2 = new Each(join2, new Fields("domain", "d-node-hash", "cluster-id", "predicate-hash", "object-domain", "object-cid"),
      new Identity(new Fields("doc-domain", "subject-domain", "subject-cid", "predicate-hash", "object-domain", "object-cid")));
    join2 = new Each(join2, new Counter(JOB_ID, JOIN2));

    // group relations with the same label and sum the frequencies
    join2 = new AggregateBy("CountRelations", Pipe.pipes(join2), new Fields(
        "doc-domain", "subject-domain", "subject-cid", "predicate-hash",
        "object-domain", "object-cid"), new CountBy(new Fields("total")));
    join2 = new Each(join2, new Counter(SUMMARY_ID, RELATIONS));
    join2 = new Each(join2, new Counter(JOB_ID, TUPLES_WRITTEN + name));
    join2 = new Pipe(name, join2);

    return join2;
  }

}
