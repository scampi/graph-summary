/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.precision;

import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import org.sindice.analytics.benchmark.cascading.precision.connectivity.ConnectivityAssembly;
import org.sindice.analytics.benchmark.cascading.precision.schema.SchemaAssembly;
import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.sindice.core.analytics.cascading.operation.CompactionBy;
import org.sindice.graphsummary.cascading.DataGraphSummaryCascade;
import org.sindice.graphsummary.cascading.JobCounters;
import org.sindice.graphsummary.cascading.ecluster.ClusterSubAssembly;
import org.sindice.graphsummary.cascading.ecluster.ClusteringFactory.ClusteringAlgorithm;
import org.sindice.graphsummary.cascading.properties.GetPropertiesGraph;
import org.sindice.graphsummary.cascading.relationships.RelationsGraph;

import cascading.operation.Identity;
import cascading.operation.state.Counter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.Unique;
import cascading.pipe.joiner.LeftJoin;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * This class creates a mapping between an evaluated summary and the gold standard,
 * from the point of view of the edges in the summary.
 * 
 * Using the entity to summary node mapping available in <b>*-entity-table</b> for both evaluated and gold standard,
 * this class computes the set of summary nodes from the gold standard that are included in nodes of the evaluated
 * summary. Then, we get the set of attributes used between super-nodes of the evaluated summary, for each edge of
 * the gold standard.
 * <p>
 * There are 6 heads, 3 for the gold standard and 3 for the evaluated summary.
 * The heads are computed using the {@link DataGraphSummaryCascade} for both summaries, and are composed of
 * the following:
 * <ul>
 * <li><b>{gold,eval}-relations</b>: the relations between summary nodes, from {@link RelationsGraph};</li>
 * <li><b>{gold,eval}-properties</b>: the properties of summary nodes, from {@link GetPropertiesGraph}; and</li>
 * <li><b>{gold,eval}-entity-table</b>: the mapping of entity to node identifier, from an implementation of
 * {@link ClusterSubAssembly}.</li>
 * </ul>
 * <p>
 * There are 2 tails:
 * <ul>
 * <li><b>eval-linkset</b>: it contains the linksets of the gold standard summary, extended with the properties
 * from the evaluated summary; and</li>
 * <li><b>gold-eval-table</b>: it contains a table with the overlay of nodes from the gold standard summary by those
 * of the evaluated summary.</li>
 * </ul>
 * The eval-linkset contains the following fields:
 * <ul>
 * <li><b>domain</b>: the domain of the node;</li>
 * <li><b>src-gold</b>: the identifier of the gold node at the tail of the edge;</li>
 * <li><b>src-count-gold</b>: the number of entities inside the tail node;</li>
 * <li><b>dst-gold</b>: the identifier of the gold node at the head of the edge;</li>
 * <li><b>dst-count-gold</b>: the number of entities inside the head node;</li>
 * <li><b>predicates-gold</b>: the set of attributes as a {@link Tuple} from the two src-gold to dst-gold nodes;</li>
 * <li><b>src-eval</b>: the identifier of the evaluated node at the tail of the edge;</li>
 * <li><b>src-eval-count</b>: the number of gold clusters in the src-eval;</li>
 * <li><b>dst-eval</b>: the identifier of the evaluated node at the head of the edge; and</li>
 * <li><b>dst-eval-count</b>: the number of gold clusters in the dst-eval;</li>
 * <li><b>predicates-eval</b>: the set of attributes as a {@link Tuple} from the src-eval to the dst-eval nodes.</li>
 * </ul>
 * The gold-eval-table contains the following fields:
 * <ul>
 * <li><b>domain</b>: the domain of the node;</li>
 * <li><b>cid-gold</b>: the identifier of the gold node;</li>
 * <li><b>cid-eval</b>: the identifier of the evaluated node;</li>
 * <li><b>c-count-gold</b>: the number of entities inside the cid-gold node;</li>
 * <li><b>types-gold</b>: the type labels associated with cid-gold;</li>
 * <li><b>properties-gold</b>: the attributes associated with cid-gold;</li>
 * <li><b>types-eval</b>: the type labels associated with cid-eval; and</li>
 * <li><b>properties-eval</b>: the attributes associated with cid-eval.</li>
 * </ul>
 * <p>
 * See Section "precision" in the IDEAS-2013 draft paper for more information (available in the resources folder).
 * 
 * @see ConnectivityAssembly
 * @see SchemaAssembly
 */
@AnalyticsName(value="linksets")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(name="gold-relations", from=RelationsGraph.class),
  @AnalyticsPipe(name="gold-properties", from=GetPropertiesGraph.class),
  @AnalyticsPipe(name="gold-entity-table", from=ClusterSubAssembly.class),
  @AnalyticsPipe(name="eval-relations", from=RelationsGraph.class),
  @AnalyticsPipe(name="eval-properties", from=GetPropertiesGraph.class),
  @AnalyticsPipe(name="eval-entity-table", from=ClusterSubAssembly.class)
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(name="eval-linkset", fields={
    "domain", "src-gold", "src-count-gold", "dst-gold", "dst-count-gold", "predicates-gold",
    "src-eval", "src-eval-count", "dst-eval", "dst-eval-count", "predicates-eval"
  }),
  @AnalyticsPipe(name="gold-eval-table", fields={
    "domain", "cid-gold", "cid-eval", "c-count-gold", "types-gold", "properties-gold", "types-eval", "properties-eval"
  })
})
public class Linksets extends AnalyticsSubAssembly {

  private static final long serialVersionUID = 5884869401800541188L;

  public Linksets() {
    super();
  }

  public Linksets(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  @Override
  protected Object assemble(Pipe[] previous, Object...args) {
    Pipe eTable = createEntityTable(previous[2], previous[5], previous[1], previous[4]);

    final Pipe goldEvalTable = new Pipe("gold-eval-table", eTable);

    Pipe linksets = getEvalLinksets(eTable, previous[0], previous[3]);

    return Pipe.pipes(linksets, goldEvalTable);
  }

  /**
   * Create an entity table with a mapping from gold to evaluated summary nodes.
   * 
   * An evaluated summary may have no mapping for some summarization algorithms, e.g., if there is no <em>type</em>
   * feature for the {@link ClusteringAlgorithm#TYPES} algorithm. In this case, we associate such entities to the
   * {@link BlankCluster#BLANK_CID} blank equivalence class. In addition, schema information stored in the
   * {@link Fields} "types-eval" and "properties-eval" are <code>null</code>.
   * @param goldETable the entity table of the gold nodes in the head "gold-entity-table"
   * @param evalETable the entity table of the eval node in the head "eval-entity-table"
   * @param goldProps the schema information of the gold nodes in the head "gold-properties"
   * @return a {@link Pipe} with mappings from gold to eval nodes
   */
  private Pipe createEntityTable(Pipe goldETable, Pipe evalETable, Pipe goldProps, Pipe evalProps) {
    goldETable = new Each(goldETable, new Fields("domain", "subject-hash", "cluster-id"),
      new Identity(new Fields("domain-gold", "node-gold", "cid-gold")));
    evalETable = new Each(evalETable, new Fields("domain", "subject-hash", "cluster-id"),
      new Identity(new Fields("domain-eval", "node-eval", "cid-eval")));

    Pipe eTable = new CoGroup("entity-table", goldETable, new Fields("domain-gold", "node-gold"),
      evalETable, new Fields("domain-eval", "node-eval"), new LeftJoin());
    eTable = new Each(eTable, new Fields("domain-gold", "cid-gold", "cid-eval"),
      new Identity(new Fields("domain", "cid-gold", "cid-eval")));
    eTable = new Unique(eTable, new Fields("domain", "cid-gold", "cid-eval"));
    eTable = new Each(eTable, new Fields("cid-eval"), new BlankCluster(new Fields("cid-eval")), Fields.SWAP);
    eTable = new Each(eTable, new Counter(JOB_ID, name + " - gold to eval table"));

    // Get the types and attributes of the gold cluster
    final Fields dec = new Fields(
      "domain", "cluster-id", "types", "properties", "c-count",
      "domain-r", "cid-gold", "cid-eval"
    );
    goldProps = new Each(goldProps, new Counter(JOB_ID, name + " - gold properties"));
    eTable = new CoGroup("e-table x gold-props", goldProps, new Fields("domain", "cluster-id"),
      eTable, new Fields("domain", "cid-gold"), dec);
    eTable = new Each(eTable, new Fields("domain", "cid-gold", "cid-eval", "c-count", "types", "properties"),
      new Identity(new Fields("domain", "cid-gold", "cid-eval", "c-count-gold", "types-gold", "properties-gold")));

    // Get the types and attributes of the eval cluster
    final Fields dec2 = new Fields(
      "domain", "cid-gold", "cid-eval", "c-count-gold", "types-gold", "properties-gold",
      "d", "cluster-id", "types", "properties", "c-count"
    );
    evalProps = new Each(evalProps, new Counter(JOB_ID, name + " - eval properties"));
    eTable = new CoGroup("e-table x eval-props", eTable, new Fields("domain", "cid-eval"),
      evalProps, new Fields("domain", "cluster-id"), dec2, new LeftJoin());

    eTable = new Each(eTable, new Fields("domain", "cid-gold", "cid-eval", "c-count-gold", "types-gold",
      "properties-gold", "types", "properties"),
    new Identity(new Fields("domain", "cid-gold", "cid-eval", "c-count-gold", "types-gold", "properties-gold",
      "types-eval", "properties-eval")));

    eTable = new Unique(eTable, new Fields("domain", "cid-gold", "cid-eval"));

    return eTable;
  }

  /**
   * Compute the eval-linkset tail.
   * 
   * There are several counters:
   * <ul>
   * <li><b>linksets - gold relations</b>: number of gold linksets between two summary nodes, regardless
   * of the predicate label;</li>
   * <li><b>linksets - gold relations (target)</b>: number of tuples after joining on the edge head with
   * the entity table;</li>
   * <li><b>linksets - gold relations (source)</b>: number of tuples after joining on the edge tail with
   * the entity table;</li>
   * <li><b>linksets - eval linksets</b>: number of linksets from the evaluated summary; and</li>
   * <li><b>linksets - final</b>: total number of tuples for the returned {@link Pipe}.</li>
   * </ul>
   * @param eTable the entity table created by {@link #createEntityTable(Pipe, Pipe, Pipe)}.
   * @param goldLinkset the {@link RelationsGraph} {@link Pipe} of the gold clusters
   * @param evalLinkset the {@link RelationsGraph} {@link Pipe} of the evaluated clusters
   * @return a {@link Pipe} with the gold standard linksets extended with properties of the evaluated summary.
   */
  private Pipe getEvalLinksets(Pipe eTable, Pipe goldLinkset, Pipe evalLinkset) {
    /*
     * Mapping from eval to a list of gold clusters
     */
    eTable = new Each(eTable, new Fields("domain", "cid-gold", "cid-eval", "c-count-gold"), new Identity());

    /*
     * Linksets of the gold clusters
     */
    goldLinkset = new AggregateBy(goldLinkset, new Fields("doc-domain", "subject-cid", "object-cid"),
        new CompactionBy(new Fields("predicate-hash"), new Fields("predicates-gold")));
    goldLinkset = new Each(goldLinkset, new Fields("doc-domain", "subject-cid", "object-cid", "predicates-gold"),
      new Identity(new Fields("domain", "src-gold", "dst-gold", "predicates-gold")));
    goldLinkset = new Each(goldLinkset, new Counter(JobCounters.JOB_ID, name + " - gold relations"));

    /*
     * Get the eval cluster(s) that overlap this dst-gold cluster
     */
    final Fields dec = new Fields(
      "domain", "src-gold", "dst-gold", "predicates-gold",
      "domain-r", "cid-gold", "cid-eval", "c-count-gold"
    );
    goldLinkset = new CoGroup("gold-linkset-dst-join", goldLinkset, new Fields("domain", "dst-gold"),
      eTable, new Fields("domain", "cid-gold"), dec);
    goldLinkset = new Each(goldLinkset,
      new Fields("domain", "src-gold", "dst-gold", "c-count-gold", "predicates-gold", "cid-eval"),
      new Identity(new Fields("domain", "src-gold", "dst-gold", "dst-count-gold", "predicates-gold", "dst-eval")));
    goldLinkset = new Each(goldLinkset, new Counter(JOB_ID, name + " - gold relations (target)"));

    /*
     * Get the eval cluster(s) that overlap this src-gold cluster
     */
    final Fields dec2 = new Fields(
      "domain", "src-gold", "dst-gold", "dst-count-gold", "predicates-gold", "dst-eval",
      "domain-r", "cid-gold", "cid-eval", "c-count-gold"
    );
    goldLinkset = new CoGroup("gold-linkset-src-join", goldLinkset, new Fields("domain", "src-gold"),
      eTable, new Fields("domain", "cid-gold"), dec2);
    goldLinkset = new Each(goldLinkset,
      new Fields("domain", "src-gold", "c-count-gold", "dst-gold", "dst-count-gold", "predicates-gold", "cid-eval", "dst-eval"),
      new Identity(new Fields("domain", "src-gold", "src-count-gold", "dst-gold", "dst-count-gold", "predicates-gold",
        "src-eval", "dst-eval")));
    goldLinkset = new Each(goldLinkset, new Counter(JOB_ID, name + " - gold relations (source)"));

    /*
     * Linksets of the eval clusters
     */
    evalLinkset = new AggregateBy(evalLinkset, new Fields("doc-domain", "subject-cid", "object-cid"),
        new CompactionBy(new Fields("predicate-hash"), new Fields("predicates-eval")));
    evalLinkset = new Each(evalLinkset, new Fields("doc-domain", "subject-cid", "object-cid", "predicates-eval"),
      new Identity(new Fields("domain", "src-eval", "dst-eval", "predicates-eval")));
    evalLinkset = new Each(evalLinkset, new Counter(JobCounters.JOB_ID, name + " - eval linksets"));

    final Fields dec3 = new Fields(
      "domain", "src-gold", "src-count-gold", "dst-gold", "dst-count-gold", "predicates-gold", "src-eval", "dst-eval",
      "domain-r", "src-eval-r", "dst-eval-r", "predicates-eval"
    );
    // left join, because an eval node can be the blank equivalence class, and it is discarded during
    // the RelationsGraph computation
    goldLinkset = new CoGroup(goldLinkset, new Fields("domain", "src-eval", "dst-eval"),
      evalLinkset, new Fields("domain", "src-eval", "dst-eval"), dec3, new LeftJoin());
    goldLinkset = new Each(goldLinkset,
      new Fields("domain", "src-gold", "src-count-gold", "dst-gold", "dst-count-gold", "predicates-gold",
        "src-eval", "dst-eval", "predicates-eval"),
      new Identity());

    /*
     * Get the total number of gold clusters in an evaluated one
     */
    Pipe evalCounts = new Pipe("eval-counts", eTable);
    evalCounts = new CountBy(evalCounts, new Fields("domain", "cid-eval"), new Fields("cid-gold"));
    evalCounts = new Each(evalCounts, new Fields("domain", "cid-eval", "cid-gold"),
      new Identity(new Fields("domain", "cid-eval", "size-eval")));

    // count of src-eval
    final Fields dec4 = new Fields(
      "domain", "src-gold", "src-count-gold", "dst-gold", "dst-count-gold", "predicates-gold",
      "src-eval", "dst-eval", "predicates-eval",
      "domain-r", "cid-eval-r", "src-eval-count"
    );
    goldLinkset = new CoGroup("eval-src-count", goldLinkset, new Fields("domain", "src-eval"),
      evalCounts, new Fields("domain", "cid-eval"), dec4);
    goldLinkset = new Each(goldLinkset, new Fields("domain", "src-gold", "src-count-gold", "dst-gold", "dst-count-gold",
      "predicates-gold", "src-eval", "src-eval-count", "dst-eval", "predicates-eval"), new Identity());

    // count of dst-eval
    final Fields dec5 = new Fields(
      "domain", "src-gold", "src-count-gold", "dst-gold", "dst-count-gold", "predicates-gold",
      "src-eval", "src-eval-count", "dst-eval", "predicates-eval",
      "domain-r", "cid-eval-r", "dst-eval-count"
    );
    goldLinkset = new CoGroup("eval-dst-count", goldLinkset, new Fields("domain", "dst-eval"),
      evalCounts, new Fields("domain", "cid-eval"), dec5);
    goldLinkset = new Each(goldLinkset, new Fields("domain", "src-gold", "src-count-gold", "dst-gold", "dst-count-gold",
      "predicates-gold", "src-eval", "src-eval-count", "dst-eval", "dst-eval-count", "predicates-eval"), new Identity());

    goldLinkset = new Each(goldLinkset, new Counter(JOB_ID, name + " - final"));
    return new Pipe("eval-linkset", goldLinkset);
  }

}
