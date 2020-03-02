/**
 * Copyright (c) 2009-2011 Sindice Limited. All Rights Reserved. Project and
 * contact information: http://www.siren.sindice.com/ This file is part of the
 * SIREn project. SIREn is a free software: you can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version. SIREn is distributed in the hope that
 * it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Affero General Public License for more details. You should have received a
 * copy of the GNU Affero General Public License along with SIREn. If not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.siren.ranking.linkanalysis.ding;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;
import org.sindice.core.analytics.linkanalysis.ding.DingFlow;
import org.sindice.core.analytics.linkanalysis.ding.DingProcess;
import org.sindice.core.analytics.linkanalysis.ding.DingStatisticsFlow;
import org.sindice.core.analytics.linkanalysis.ding.LfIdfAggregator;
import org.sindice.core.analytics.testHelper.AbstractAnalyticsTestCase;
import org.sindice.core.analytics.util.SequenceFilesIterator;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.WritableSequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.TupleEntry;

public class TestDingProcess
extends AbstractAnalyticsTestCase {

  private final File inputGraphSummary = new File("./src/test/resources/graph-2011-10-11_sample");
  private Tap<JobConf, RecordReader, OutputCollector> source;

  @Override
  @Before
  public void setUp()
  throws Exception {
    super.setUp();
    source = new Hfs(new SequenceFile(DingProcess.SUMMARY_FIELDS),
      inputGraphSummary.getAbsolutePath());
  }

  @Test
  public void testDatasetsStatisticsFlow()
  throws URISyntaxException, IOException {
    AppProps.setApplicationJarClass(properties, DingStatisticsFlow.class);
    final DingStatisticsFlow dStats = new DingStatisticsFlow(new Pipe("summary"));
    final HashMap<String, Tap> sinks = dStats.getSinks(testOutput.getAbsolutePath());
    final Flow dStatsFlow = new HadoopFlowConnector(properties).connect(source, sinks, dStats);
    dStatsFlow.complete();

    final Path pDist = new Path(sinks.get("Predicate-Dist").getIdentifier());
    final Path d2i = new Path(sinks.get("Dataset2Index").getIdentifier());
    final Path dsNodeCard = new Path(sinks.get("Dataset-Node-Cardinality").getIdentifier());

    checkPredicateDistribution(pDist);
    checkDataset2Index(dStatsFlow, d2i);
    checkDatasetsCard(dStatsFlow, dsNodeCard, d2i);
  }

  private void checkDataset2Index(Flow dStatsFlow, Path d2i)
  throws IOException {
    final HashSet<Integer> indexes = new HashSet<Integer>();
    final long nb = dStatsFlow.getFlowStats().getCounterValue("Datasets", "Cardinality");
    final SequenceFilesIterator it = new SequenceFilesIterator(d2i, DingStatisticsFlow.D2I_FIELDS);

    try {
      while (it.hasNext()) {
        final TupleEntry te = it.next();
        final int index = te.getInteger("index");
        assertFalse(indexes.contains(index));
        indexes.add(index);
      }
    } finally {
      it.close();
    }
    assertEquals(nb, indexes.size());
  }

  private void checkDatasetsCard(Flow dStatsFlow, Path dsNodeCard, Path d2i)
  throws IOException {
    final long nb = dStatsFlow.getFlowStats().getCounterValue("Datasets", "Cardinality");
    final SequenceFilesIterator itD2I = new SequenceFilesIterator(d2i, DingStatisticsFlow.D2I_FIELDS);
    final SequenceFilesIterator itDsCard = new SequenceFilesIterator(dsNodeCard, DingStatisticsFlow.D_NODE_CARD);
    final HashMap<Integer, String> i2d = new HashMap<Integer, String>();

    assertEquals(10, nb);
    try {
      while (itD2I.hasNext()) {
        final TupleEntry te = itD2I.next();
        final String dataset = te.getString("dataset");
        final int index = te.getInteger("index");
        assertFalse(i2d.containsKey(index));
        i2d.put(index, dataset);
      }
      while (itDsCard.hasNext()) {
        final TupleEntry e = itDsCard.next();
        final int card = e.getInteger("nodes-cardinality");
        final int di = e.getInteger("dataset");
        final String dataset = i2d.get(di);
        assertTrue(dataset != null);
        if (dataset.equals("dbpedia.org")) {
          assertEquals(23, card);
        } else if (dataset.equals("faviki.com")) {
          assertEquals(1, card);
        } else if (dataset.equals("freebase.com")) {
          assertEquals(4, card);
        } else if (dataset.equals("fwwebdev.com")) {
          assertEquals(1, card);
        } else if (dataset.equals("gov.tw")) {
          assertEquals(1, card);
        } else if (dataset.equals("nytimes.com")) {
          assertEquals(1, card);
        } else if (dataset.equals("ookaboo.com")) {
          assertEquals(6, card);
        } else if (dataset.equals("w3.org")) {
          assertEquals(1, card);
        } else if (dataset.equals("wikipedia.org")) {
          assertEquals(2, card);
        } else if (dataset.equals("zitgist.com")) {
          assertEquals(1, card);
        } else {
          fail();
        }
      }
    } finally {
      itD2I.close();
      itDsCard.close();
    }
  }

  private void checkPredicateDistribution(Path pDist)
  throws IOException {
    final HashMap<String, Long> pf = new HashMap<String, Long>();
    final SequenceFilesIterator it = new SequenceFilesIterator(pDist, DingStatisticsFlow.PDIST_FIELDS);

    try {
      while (it.hasNext()) {
        final TupleEntry te = it.next();
        pf.put(te.getString("predicate-hash"), te.getLong("datasets"));
      }
    } finally {
      it.close();
    }
    // Expected numbers of predicates in the whole dataset layer.
    assertEquals(9, pf.size());

    final String[] pfExpected = { "http://dbpedia.org/ontology/genus 1",
                                  "http://dbpedia.org/ontology/producer 1",
                                  "http://dbpedia.org/property/wordnet_type 1",
                                  "http://rdfs.org/sioc/ns#topic 1",
                                  "http://semantictagging.org/ns#means 1",
                                  "http://www.w3.org/2002/07/owl#sameAs 3",
                                  "http://xmlns.com/foaf/0.1/depicts 1",
                                  "http://xmlns.com/foaf/0.1/interest 1",
                                  "http://xmlns.com/foaf/0.1/page 1" };
    for (String p : pfExpected) {
      final String[] part = p.split(" ", 2);
      assertTrue(pf.containsKey(part[0]));
      assertEquals(Integer.valueOf(part[1]).intValue(), pf.get(part[0]).intValue());
    }
  }

  @Test
  public void testDingMatrix()
  throws Exception {
    AppProps.setApplicationJarClass(properties, DingStatisticsFlow.class);
    final DingStatisticsFlow dStats = new DingStatisticsFlow(new Pipe("summary"));
    final HashMap<String, Tap> sinks = dStats.getSinks(testOutput.getAbsolutePath());
    final Flow dStatsFlow = new HadoopFlowConnector(properties).connect(source, sinks, dStats);
    dStatsFlow.complete();

    // Set the number of datasets property
    final long nb = dStatsFlow.getFlowStats().getCounterValue("Datasets", "Cardinality");
    properties.setProperty(LfIdfAggregator.NUM_VERTICES, Long.toString(nb));

    // Set the teleportation probability
    properties.setProperty(DingProcess.TELEPORTATION_PROBA, "0.8");

    final String d2iPath = sinks.get("Dataset2Index").getIdentifier();
    final String pDistPath = sinks.get("Predicate-Dist").getIdentifier();

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
    final Tap<JobConf, RecordReader, OutputCollector> sink = new Hfs(new SequenceFile(DingFlow.DECLARATION_FIELDS), testOutput.getAbsolutePath() + "/dingmatrix");
    final Flow dLayerFlow = new HadoopFlowConnector(properties).connect(sources, sink, dLayer);
    dLayerFlow.complete();

    final SequenceFilesIterator it = new SequenceFilesIterator(new Path(testOutput.getAbsolutePath() + "/dingmatrix"), DingFlow.DECLARATION_FIELDS);
    try {
      while (it.hasNext()) {
        final TupleEntry e = it.next();
        System.out.println(e);
      }
    } finally {
      it.close();
    }
  }

  @Test
  public void testDatasetDingRank()
  throws Exception {
    AppProps.setApplicationJarClass(properties, DingStatisticsFlow.class);
    final DingStatisticsFlow dStats = new DingStatisticsFlow(new Pipe("summary"));
    final HashMap<String, Tap> sinks = dStats.getSinks(testOutput.getAbsolutePath());
    final Flow dStatsFlow = new HadoopFlowConnector(properties).connect(source, sinks, dStats);
    dStatsFlow.complete();

    // Set the number of datasets property
    final long numVertices = dStatsFlow.getFlowStats().getCounterValue("Datasets", "Cardinality");
    properties.setProperty(LfIdfAggregator.NUM_VERTICES, Long.toString(numVertices));

    // Set the teleportation probability
    properties.setProperty(DingProcess.TELEPORTATION_PROBA, "0.8");

    final String d2iPath = sinks.get("Dataset2Index").getIdentifier();
    final String pDistPath = sinks.get("Predicate-Dist").getIdentifier();

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
    final Tap<JobConf, RecordReader, OutputCollector> sink = new Hfs(new WritableSequenceFile(DingFlow.DECLARATION_FIELDS, IntWritable.class, VectorWritable.class), testOutput.getAbsolutePath() + "/matrix");
    final Flow dLayerFlow = new HadoopFlowConnector(properties).connect(sources, sink, dLayer);
    dLayerFlow.complete();

    // Iteration to compute the DING rank
    final Path dsNodeCard = new Path(sinks.get("Dataset-Node-Cardinality").getIdentifier());
    final DingProcess proc = new DingProcess(properties, inputGraphSummary
    .getAbsolutePath(), testOutput.getAbsolutePath());
    proc.computeDingRank((int) numVertices, new Path(testOutput.getAbsolutePath(), "matrix"), dsNodeCard);
  }

}
