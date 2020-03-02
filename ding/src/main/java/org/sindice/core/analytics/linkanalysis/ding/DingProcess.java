/**
 * Copyright 2012, Campinas Stephane
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.sindice.core.analytics.linkanalysis.ding;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.MatrixSlice;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import org.sindice.core.analytics.linkanalysis.ding.mahout.SindiceDistributedRowMatrix;
import org.sindice.core.analytics.util.SequenceFilesIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import riffle.process.ProcessComplete;
import riffle.process.ProcessPrepare;
import riffle.process.ProcessStop;
import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.WritableSequenceFile;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/**
 * 
 * @author Stephane Campinas [10 Sep 2012]
 * @email stephane.campinas@deri.org
 *
 */
@riffle.process.Process
public class DingProcess
implements Comparable<DingProcess> {

  public static final String FLOW_NAME = "DING";
  private final static Logger logger         = LoggerFactory
                                             .getLogger(DingProcess.class);

  public static final Fields SUMMARY_FIELDS =
    new Fields("doc-domain", "sub-domain", "sub-types", "predicate-hash",
               "obj-domain", "obj-types", "total");

  private final Properties properties = new Properties();
  private final String summaryPath;
  private final String outputPath;

  /**
   * DING parameters
   */
  public static final String  TELEPORTATION_PROBA = "teleportation-probability";
  private final double DEFAULT_TELEPORTATION_PROBA = 0.8;
  private final double teleportationProbability;

  public static final String CONVERGENCE_THRESHOLD = "cv-threshold";
  private final double DEFAULT_THRESHOLD = 1e-8;
  private final double threshold;

  public DingProcess(final Properties p,
                     final String summaryPath,
                     final String outputPath) {
    properties.putAll(p);
    this.summaryPath = summaryPath;
    this.outputPath = outputPath;
    // Set the teleportation probability
    if (!properties.containsKey(TELEPORTATION_PROBA)) {
      properties.setProperty(TELEPORTATION_PROBA, Double.toString(DEFAULT_TELEPORTATION_PROBA));
      teleportationProbability = DEFAULT_TELEPORTATION_PROBA;
    } else {
      teleportationProbability = Double.valueOf(properties.getProperty(TELEPORTATION_PROBA));
    }
    // Set the Convergence threshold
    if (!properties.containsKey(CONVERGENCE_THRESHOLD)) {
      threshold = DEFAULT_THRESHOLD;
    } else {
      threshold = Double.valueOf(properties.getProperty(CONVERGENCE_THRESHOLD));
    }
  }

  @ProcessPrepare
  public void prepare() {
    
  }

  @ProcessComplete
  public void complete()
  throws IOException {
    final Tap<JobConf, RecordReader, OutputCollector> source = new Hfs(new SequenceFile(SUMMARY_FIELDS), summaryPath);

    /*
     * Compute Datasets statistics
     */
    AppProps.setApplicationJarClass(properties, DingStatisticsFlow.class);
    final DingStatisticsFlow dStats = new DingStatisticsFlow(new Pipe("summary"));
    final HashMap<String, Tap> sinks = dStats.getSinks(outputPath);
    final Flow dStatsFlow = new HadoopFlowConnector(properties).connect(source, sinks, dStats);
    dStatsFlow.complete();

    final String d2iPath = sinks.get("Dataset2Index").getIdentifier();
    final String pDistPath = sinks.get("Predicate-Dist").getIdentifier();

    // Set the number of datasets property
    final long numVertices = dStatsFlow.getFlowStats().getCounterValue("Datasets", "Cardinality");
    properties.setProperty(LfIdfAggregator.NUM_VERTICES, Long.toString(numVertices));
    logger.info("Computing DING rank over {} datasets", numVertices);

    /*
     * Compute the DING matrix
     */
    AppProps.setApplicationJarClass(properties, DingFlow.class);
    final DingFlow dLayer = new DingFlow(new Pipe("summary"),
      new Pipe("Dataset2Index"), new Pipe("Predicate-Dist"));
    final HashMap<String, Tap> sources = new HashMap<String, Tap>();
    sources.put("summary", source);
    sources.put("Dataset2Index", new Hfs(new SequenceFile(DingStatisticsFlow.D2I_FIELDS), d2iPath));
    sources.put("Predicate-Dist", new Hfs(new SequenceFile(DingStatisticsFlow.PDIST_FIELDS), pDistPath));
    final Tap<JobConf, RecordReader, OutputCollector> sink = new Hfs(new WritableSequenceFile(DingFlow.DECLARATION_FIELDS, IntWritable.class, VectorWritable.class), outputPath + "/matrix");
    final Flow dLayerFlow = new HadoopFlowConnector(properties).connect(sources, sink, dLayer);
    dLayerFlow.complete();

    // Iteration to compute the DING rank
    final Path dsNodeCard = new Path(sinks.get("Dataset-Node-Cardinality").getIdentifier());
    final Vector dingRank = computeDingRank((int) numVertices, new Path(outputPath, "matrix"), dsNodeCard);
    mapIndex2Dataset(dingRank, new Path(d2iPath));
  }

  public void mapIndex2Dataset(Vector rank, Path d2i)
  throws IOException {
    final SequenceFilesIterator d2iIt = new SequenceFilesIterator(d2i, DingStatisticsFlow.D2I_FIELDS);
    final TupleEntryCollector col = new Hfs(new SequenceFile(new Fields("dataset", "dingrank")),
      outputPath + "/dingrank", SinkMode.REPLACE).openForWrite(new HadoopFlowProcess());

    try {
      while (d2iIt.hasNext()) {
        final TupleEntry te = d2iIt.next();
        final String dataset = te.getString("dataset");
        final int index = te.getInteger("index");
        col.add(new Tuple(dataset, rank.get(index)));
      }
    } finally {
      d2iIt.close();
      col.close();
    }
  }

  public Vector computeDingRank(int numVertices,
                                Path transitionMatrix,
                                Path dsNodeCard)
  throws IOException {
    DistributedRowMatrix matrix = new SindiceDistributedRowMatrix(transitionMatrix,
      new Path(outputPath), numVertices, numVertices);
    matrix.setConf(new Configuration());

    // Compute a for the power method from the book
    final Vector a = new DenseVector(numVertices).assign(1);
    final Iterator<MatrixSlice> ms = matrix.iterateAll();
    final DenseVector ones = new DenseVector(numVertices);
    ones.assign(1);
    while (ms.hasNext()) {
      final MatrixSlice m = ms.next();
      a.set(m.index(), 0);
      final double rowSum = ones.dot(m.vector()) / teleportationProbability;
      if (rowSum < 0.999999 || rowSum > 1.0000001) {
        throw new RuntimeException("The row " + m.index() + " doesn't sum up to 1: " + rowSum);
      }
    }
    
    Vector dingRank = new DenseVector(numVertices).assign(1.0 / numVertices);

    matrix = matrix.transpose();

    /*
     * initialize the dampling vector: each component is the quotient of the dataset
     * size divided by the total collection size. 
     */
    Vector damplingVector = new DenseVector(numVertices);
    // get the size of datasets
    final SequenceFilesIterator dsIt = new SequenceFilesIterator(dsNodeCard, DingStatisticsFlow.D_NODE_CARD);
    try {
      while (dsIt.hasNext()) {
        final TupleEntry te = dsIt.next();
        damplingVector.set(te.getInteger("dataset"), te.getLong("nodes-cardinality"));
      }
    } finally {
      dsIt.close();
    }
    damplingVector = damplingVector.normalize(1);

    // Iteration to compute the DING rank
    int numIterations = 0;
    double maxDelta = 0;
    do {
      logger.info("Starting iteration {}", ++numIterations);

      final double powerMethod = dingRank.dot(a) * teleportationProbability + (1.0 - teleportationProbability);
      final Vector nextDingRank = matrix.times(dingRank).plus(damplingVector.times(powerMethod));
      checkRankSum(nextDingRank);
      // Converge?
      final Iterator<Element> it = dingRank.iterateNonZero();
      maxDelta = 0;
      while (it.hasNext()) {
        final Element e = it.next();
        if (Math.abs(e.get() - nextDingRank.get(e.index())) / e.get() > maxDelta)
          maxDelta = Math.abs(e.get() - nextDingRank.get(e.index())) / e.get();
      }
      dingRank = nextDingRank;
      logger.info("delta={}", maxDelta);
    } while (maxDelta > threshold);
    logger.info("Achieved convergence with threshold at {}, under {} iterations", threshold, numIterations);
    checkRankSum(dingRank);
    return dingRank;
  }

  /**
   * Check the sum of the vector, and check for infinite/NaN values.
   * @param rank the PageRank vector
   */
  private void checkRankSum(Vector rank) {
    final Iterator<Element> nextIt = rank.iterateNonZero();
    double sum = 0;

    while (nextIt.hasNext()) {
      final Element e = nextIt.next();
      if (Double.isInfinite(e.get())) {
        logger.info("Value at index={} is infinite", e.index());
        throw new RuntimeException();
      } else if (Double.isNaN(e.get())) {
        logger.info("Value at index={} is NaN", e.index());
        throw new RuntimeException();
      }
      sum += e.get();
    }
    if (sum < 0.999999 || sum > 1.0000001) {
      throw new RuntimeException("Incorrect Ding Rank vector, got sum=" + sum + " instead of 1.");
    }
  }

  @ProcessStop
  public void stop() {
    
  }

  @Override
  public int compareTo(DingProcess d) {
    if (d.outputPath.equals(outputPath) && d.summaryPath.equals(summaryPath) &&
        d.teleportationProbability == teleportationProbability &&
        d.threshold == threshold && d.properties.equals(properties)) {
      return 0;
    }
    return 1;
  }

}
