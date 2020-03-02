/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster.fbisimulation;

import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.sindice.core.analytics.cascading.riffle.ProcessStats;
import org.sindice.graphsummary.cascading.ecluster.ClusterSubAssembly;
import org.sindice.graphsummary.cascading.ecluster.fbisimulation.adjacency.AdjacencyListAssembly;
import org.sindice.graphsummary.cascading.ecluster.fbisimulation.splitting.SplitBuffer;
import org.sindice.graphsummary.cascading.ecluster.fbisimulation.splitting.SplittingAssembly;
import org.sindice.graphsummary.cascading.entity.GetClusterGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import riffle.process.DependencyIncoming;
import riffle.process.DependencyOutgoing;
import riffle.process.Process;
import riffle.process.ProcessCleanup;
import riffle.process.ProcessComplete;
import riffle.process.ProcessStop;
import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.stats.FlowStats;
import cascading.tap.MultiSourceTap;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.util.Util;

/**
 * This {@link Process} {@link Flow} computes a clustering based on the forward-bisimulation equivalence relation. The
 * algorithm is based on the <a href="http://dl.acm.org/citation.cfm?id=37186">Relational Coarsest Partition</a>.
 * <p>
 * This operation takes as input the tail of {@link GetClusterGraph}, and outputs a tail as in
 * {@link ClusterSubAssembly}.
 * <p>
 * This process performs the following operations:
 * <ul>
 * <li>prepare the data using {@link AdjacencyListAssembly};</li>
 * <li>perform a sequence of {@link SplittingAssembly splitting operations}, which are stopped when
 * {@link SplitBuffer#SPLITS} is equal to 0; and</li>
 * <li>post-process the data so that it matches the expected tail definition of {@link ClusterSubAssembly}.</li>
 * </ul>
 */
@Process
@SuppressWarnings("rawtypes")
@AnalyticsName(value="cluster-generator")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(from=GetClusterGraph.class)
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "domain", "subject-hash", "spo-in", "spo-out", "cluster-id" })
})
public class FBisimulationProcess {

  private static final Logger       logger      = LoggerFactory.getLogger(FBisimulationProcess.class);

  // Holds intermediate data
  private String                    tmpPath;

  private Tap                       intraSet;
  private static String             INTRA_SET   = "intra";
  private Tap                       sourcesSet;
  private static String             SOURCES_SET = "sources";
  private Tap                       sinksSet;
  private static String             SINKS_SET   = "sinks";

  // Used for filtering the clusters which incoming set should be joined in SplittingAssembly
  private static String             SPLITS      = "splits";

  private final Tap                 source;
  private final Tap                 sink;
  private final Pipe                head;
  private final Map<Object, Object> properties  = new HashMap<Object, Object>();
  private final JobConf             conf;

  private final List<FlowStats>     stats       = new LinkedList<FlowStats>(); // maintain order

  /**
   * Creates a new {@link FBisimulationProcess} {@link Process}.
   * @param source the source {@link Tap}
   * @param sink the sink {@link Tap}
   * @param properties the {@link Properties} of the process
   */
  public FBisimulationProcess(final Tap source, final Tap sink, final Properties properties) {
    this(Tap.taps(source), Tap.taps(sink), Pipe.pipes(new Pipe(Analytics.getName(GetClusterGraph.class))), properties);
  }

  /**
   * Creates a new {@link FBisimulationProcess} {@link Process}.
   * @param sources the sources {@link Tap}s
   * @param sinks the sinks {@link Tap}s
   * @param heads the heads of the process
   * @param properties the {@link Properties} of the process
   */
  public FBisimulationProcess(final Tap[] sources, final Tap[] sinks, final Pipe[] heads, final Properties properties) {
    this.source = sources[0];
    this.sink = sinks[0];
    this.head = heads[0];
    this.properties.putAll(properties);
    conf = HadoopUtil.createJobConf(this.properties, new JobConf());
  }

  /**
   * Set the path to the temporary working directory.
   */
  public void setWorkingDir(String path) {
    tmpPath = path;
  }

  /**
   * Return the name of the working folder for this {@link FBisimulationProcess}.
   * The name is unique across simultaneous runs of {@link FBisimulationProcess}.
   */
  private String getWorkingDir() {
    if (tmpPath == null) {
      tmpPath = Util.createUniqueID();
    }
    logger.debug("Using the temporary path at {}", tmpPath);
    return tmpPath;
  }

  /**
   * Get the path to the named folder in the working directory.
   * @param name the name of the folder
   * @return the {@link Path} to the folder
   */
  private String getTmpPath(final String name) {
    return new Path(getWorkingDir(), name).toString();
  }

  @ProcessStats
  public List<FlowStats> getStats() {
    return stats;
  }

  @ProcessCleanup
  public void cleanup()
  throws IOException {
    final Path path = new Path(getWorkingDir());
    final FileSystem fs = FileSystem.get(conf);
    fs.delete(path, true);
  }

  @DependencyIncoming
  public Tap getSource() {
    return source;
  }

  @DependencyOutgoing
  public Tap getSink() {
    return sink;
  }

  @SuppressWarnings("unchecked")
  @ProcessComplete
  public void run()
  throws IOException {
    /*
     * Get the incoming adjacency set of each entity. Entities that are sources or sinks are filtered
     * from the tap on which the iteration will be performed. Cluster identifiers of sink entities won't change, and
     * the cluster identifier of source entities can be updated only at the last moment.
     */
    final Map<String, Fields> adjTails = Analytics.getTailsFields(AdjacencyListAssembly.class);
    intraSet = new Hfs(new SequenceFile(adjTails.get(INTRA_SET)), getTmpPath(INTRA_SET));
    sourcesSet = new Hfs(new SequenceFile(adjTails.get(SOURCES_SET)), getTmpPath(SOURCES_SET));
    sinksSet = new Hfs(new SequenceFile(adjTails.get(SINKS_SET)), getTmpPath(SINKS_SET));
    final Map<String, Tap> adjSinks = new HashMap<String, Tap>();
    adjSinks.put(INTRA_SET, intraSet);
    adjSinks.put(SOURCES_SET, sourcesSet);
    adjSinks.put(SINKS_SET, sinksSet);

    final AdjacencyListAssembly adj = new AdjacencyListAssembly(Pipe.pipes(head));
    final Flow adjFlow = new HadoopFlowConnector(properties).connect(Analytics.getName(adj), source, adjSinks, adj);
    adjFlow.complete();
    stats.add(adjFlow.getFlowStats());

    /*
     * Splitting operation of the clusters
     */
    int iteration = 1;
    long nbSplits = -1;
    final SplittingAssembly splitting = new SplittingAssembly();
    final Fields splitFields = Analytics.getTailsFields(splitting).get("splits-new");
    final Fields partitionFields = Analytics.getTailsFields(splitting).get("partition-new");

    final Tap finalPartition; // This tap contains the final partition of the intra set.
    Tap splits = intraSet; // for the first iteration, the splits is the same set as the intra
    while (true) {
      logger.info("Iteration {}", iteration);

      // Split the clusters that are unstable
      final HashMap<String, Tap> splitSources = new HashMap<String, Tap>();
      if (nbSplits == 0) { // last iteration to update the cluster identifier of the sources set
        // All the taps in a MultiSourceTap must be of the same type
        final Tap tap = new MultiSourceTap(intraSet, new MultiSourceTap(sourcesSet));
        splitSources.put("partition-old", tap);
        splitSources.put("splits-old", tap);
      } else {
        splitSources.put("partition-old", intraSet);
        splitSources.put("splits-old", splits);
      }

      final Tap newPartition = new Hfs(new SequenceFile(partitionFields),
        getTmpPath(INTRA_SET + iteration), SinkMode.REPLACE);
      final Tap newSplits = new Hfs(new SequenceFile(splitFields),
        getTmpPath(SPLITS + iteration), SinkMode.REPLACE);
      final HashMap<String, Tap> splitSinks = new HashMap<String, Tap>();
      splitSinks.put("partition-new", newPartition);
      splitSinks.put("splits-new", newSplits);

      final Flow splitFlow = new HadoopFlowConnector(properties).connect(Analytics.getName(splitting) + "-" + iteration,
        splitSources, splitSinks, splitting);
      splitFlow.complete();
      stats.add(splitFlow.getFlowStats());

      // remove the previous outputs
      if (intraSet instanceof MultiSourceTap) {
        final MultiSourceTap m = (MultiSourceTap) intraSet;
        final Iterator<Tap> it =  m.getChildTaps();
        while (it.hasNext()) {
          it.next().deleteResource(conf);
        }
      } else {
        intraSet.deleteResource(conf);
      }
      splits.deleteResource(conf);

      if (nbSplits == 0) { // last iteration to update the cluster identifier of the sources set
        finalPartition = new MultiSourceTap(newPartition, newSplits, this.sinksSet);
        break;
      }
      nbSplits = splitFlow.getStats().getCounterValue(JOB_ID, SplitBuffer.SPLITS);

      intraSet = new MultiSourceTap(newPartition, newSplits); // contains the whole intra set
      splits = newSplits; // only the portion of the intra set that was splitted
      iteration++;
    }

    /*
     * Get back the SPO field
     */
    final Pipe[] finalizeHeads = Pipe.pipes(head, new Pipe("partition"));
    final FinalizeAssembly pipe = new FinalizeAssembly(finalizeHeads);
    final Map<String, Tap> finalSources = new HashMap<String, Tap>();
    finalSources.put("partition", finalPartition);
    finalSources.put(head.getName(), source);
    final Flow finalFlow = new HadoopFlowConnector(properties)
    .connect(Analytics.getName(FinalizeAssembly.class), finalSources, sink, pipe);
    finalFlow.complete();
    stats.add(finalFlow.getFlowStats());
  }

  @ProcessStop
  public void stop() {}

}
