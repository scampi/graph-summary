/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster;

import org.junit.Test;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.riffle.RiffleProcessFlow;
import org.sindice.core.analytics.testHelper.iotransformation.UnitOfWorkTestHelper;
import org.sindice.core.analytics.testHelper.iotransformation.TupleType;
import org.sindice.graphsummary.AbstractSummaryTestCase;
import org.sindice.graphsummary.cascading.ecluster.fbisimulation.FBisimulationProcess;
import org.sindice.graphsummary.cascading.ecluster.fbisimulation.FinalizeAssembly;
import org.sindice.graphsummary.cascading.ecluster.fbisimulation.adjacency.AdjacencyListAssembly;
import org.sindice.graphsummary.cascading.ecluster.fbisimulation.splitting.SplitBuffer;
import org.sindice.graphsummary.cascading.ecluster.fbisimulation.splitting.SplittingAssembly;
import org.sindice.graphsummary.cascading.entity.GetClusterGraph;
import org.sindice.graphsummary.iotransformation.EntityDescriptionType;
import org.sindice.graphsummary.iotransformation.FBisimulationCidType;
import org.sindice.graphsummary.iotransformation.Hash128Type;
import org.sindice.graphsummary.iotransformation.Hash64Type;

import cascading.pipe.CoGroup;

public class TestFBisimulation
extends AbstractSummaryTestCase {

  /**
   * Simple test for {@link AdjacencyListAssembly}
   */
  @Test
  public void testAdjacencyList()
  throws Exception {
    runAdjacencyListAssembly("testAdjacencyList");
  }

  /**
   * Test for entities with multiple outgoing entities, which are into a same cluster
   */
  @Test
  public void testOneToManyNodeDegree()
  throws Exception {
    runAdjacencyListAssembly("testOneToManyNodeDegree");
  }

  /**
   * Test for loops, i.e., a triple where the subject and object are equals.
   */
  @Test
  public void testLoop()
  throws Exception {
    runAdjacencyListAssembly("testLoop");
  }

  /**
   * Simple test for {@link SplittingAssembly}
   */
  @Test
  public void testSplittingAssembly()
  throws Exception {
    final String inSource = RES + "testFBisimulation/testSplittingAssembly/input.check";
    final String outSource1 = RES + "testFBisimulation/testSplittingAssembly/output1.check";
    final String outSource2 = RES + "testFBisimulation/testSplittingAssembly/output2.check";

    runSplittingAssembly(inSource, inSource, outSource1, outSource2);
  }

  /**
   * Test in {@link SplittingAssembly} for the {@link CoGroup} between the entities and the incoming set.
   * When an entity points to several entities from a same cluster, the right side has duplicates.
   */
  @Test
  public void testSplittingAssembly2()
  throws Exception {
    final String inSource0 = RES + "testFBisimulation/testSplittingAssembly2/input0.check";
    final String inSource1 = RES + "testFBisimulation/testSplittingAssembly2/input1.check";
    final String outSource1 = RES + "testFBisimulation/testSplittingAssembly2/output1.check";
    final String outSource2 = RES + "testFBisimulation/testSplittingAssembly2/output2.check";

    runSplittingAssembly(inSource0, inSource1, outSource1, outSource2);
  }

  /**
   * Test the cluster ID update algorithm in {@link SplitBuffer}.
   * <p>
   * This tests all the phases: init, first split, consequent splits.
   */
  @Test
  public void testSplitBuffer1()
  throws Exception {
    final String inSource = RES + "testFBisimulation/testSplitBuffer1/input.check";
    final String outSource1 = RES + "testFBisimulation/testSplitBuffer1/output1.check";
    final String outSource2 = RES + "testFBisimulation/testSplitBuffer1/output2.check";

    runSplittingAssembly(inSource, inSource, outSource1, outSource2);
  }

  /**
   * Test the cluster ID update algorithm in {@link SplitBuffer}.
   * <p>
   * This tests when there is no split.
   */
  @Test
  public void testSplitBuffer2()
  throws Exception {
    final String inSource0 = RES + "testFBisimulation/testSplitBuffer2/input0.check";
    final String inSource1 = RES + "testFBisimulation/testSplitBuffer2/input1.check";
    final String outSource1 = RES + "testFBisimulation/testSplitBuffer2/output1.check";
    final String outSource2 = RES + "testFBisimulation/testSplitBuffer2/output2.check";

    runSplittingAssembly(inSource0, inSource1, outSource1, outSource2);
  }

  /**
   * Test the cluster ID update algorithm in {@link SplitBuffer}.
   * <p>
   * This tests when an entity has multiple outgoing links, which results in several tuples after the CoGroup.
   */
  @Test
  public void testSplitBuffer3()
  throws Exception {
    final String inSource0 = RES + "testFBisimulation/testSplitBuffer3/input0.check";
    final String inSource1 = RES + "testFBisimulation/testSplitBuffer3/input1.check";
    final String outSource1 = RES + "testFBisimulation/testSplitBuffer3/output1.check";
    final String outSource2 = RES + "testFBisimulation/testSplitBuffer3/output2.check";

    runSplittingAssembly(inSource0, inSource1, outSource1, outSource2);
  }

  /**
   * Test the cluster ID update algorithm in {@link SplitBuffer}.
   * <p>
   * This tests when a cluster is split in 2, with the last cluster having only one entity.
   */
  @Test
  public void testSplitBuffer4()
  throws Exception {
    final String inSource0 = RES + "testFBisimulation/testSplitBuffer4/input0.check";
    final String inSource1 = RES + "testFBisimulation/testSplitBuffer4/input1.check";
    final String outSource1 = RES + "testFBisimulation/testSplitBuffer4/output1.check";
    final String outSource2 = RES + "testFBisimulation/testSplitBuffer4/output2.check";

    runSplittingAssembly(inSource0, inSource1, outSource1, outSource2);
  }

  /**
   * Test the cluster ID update algorithm in {@link SplitBuffer}.
   * <p>
   * This tests when a cluster is split in 2, with the last cluster having only one entity.
   */
  @Test
  public void testSplitBuffer5()
  throws Exception {
    final String inSource0 = RES + "testFBisimulation/testSplitBuffer5/input0.check";
    final String inSource1 = RES + "testFBisimulation/testSplitBuffer5/input1.check";
    final String outSource1 = RES + "testFBisimulation/testSplitBuffer5/output1.check";
    final String outSource2 = RES + "testFBisimulation/testSplitBuffer5/output2.check";

    runSplittingAssembly(inSource0, inSource1, outSource1, outSource2);
  }

  /**
   * Test the cluster ID update algorithm in {@link SplitBuffer}.
   * <p>
   * This tests when a cluster is split in 3, with one part having no outgoing relation.
   */
  @Test
  public void testSplitBuffer6()
  throws Exception {
    final String inSource = RES + "testFBisimulation/testSplitBuffer6/input.check";
    final String outSource1 = RES + "testFBisimulation/testSplitBuffer6/output1.check";
    final String outSource2 = RES + "testFBisimulation/testSplitBuffer6/output2.check";

    runSplittingAssembly(inSource, inSource, outSource1, outSource2);
  }

  /**
   * Test for clusters where only a portion is linked to another cluster.
   * This is a test for when it happens in {@link SplittingAssembly}.
   */
  @Test
  public void testHalfClusterWithoutOutgoingLinks()
  throws Exception {
    final String inSource = RES + "testFBisimulation/testHalfClusterWithoutOutgoingLinks/input.check";
    final String outSource1 = RES + "testFBisimulation/testHalfClusterWithoutOutgoingLinks/output1.check";
    final String outSource2 = RES + "testFBisimulation/testHalfClusterWithoutOutgoingLinks/output2.check";

    runSplittingAssembly(inSource, inSource, outSource1, outSource2);
  }

  /**
   * Test for clusters where only a portion is linked to another cluster.
   * This is a test for when it happens in {@link AdjacencyListAssembly}.
   */
  @Test
  public void testHalfClusterWithoutOutgoingLinks2()
  throws Exception {
    runAdjacencyListAssembly("testHalfClusterWithoutOutgoingLinks2");
  }

  /**
   * Test {@link FinalizeAssembly}
   */
  @Test
  public void testFinalizeAssembly()
  throws Exception {
    final String inSource0 = RES + "testFBisimulation/testFinalizeAssembly/input0.check";
    final String inSource1 = RES + "testFBisimulation/testFinalizeAssembly/input1.check";
    final String outSource = RES + "testFBisimulation/testFinalizeAssembly/output.check";

    properties.setProperty(TupleType.FIELD_TYPE, Hash128Type.class.getName());
    Class<?>[] typesInput0 = { Hash64Type.class, Hash128Type.class, EntityDescriptionType.class, EntityDescriptionType.class };
    Class<?>[] typesInput1 = { Hash64Type.class, Hash128Type.class, TupleType.class, FBisimulationCidType.class };
    Class<?>[] typesOutput = { Hash64Type.class, Hash128Type.class, EntityDescriptionType.class, EntityDescriptionType.class, FBisimulationCidType.class };

    UnitOfWorkTestHelper.addSource(Analytics.getName(GetClusterGraph.class), inSource0, typesInput0);
    UnitOfWorkTestHelper.addSource("partition", inSource1, typesInput1);
    UnitOfWorkTestHelper.addSink(Analytics.getName(FinalizeAssembly.class), outSource, typesOutput);
    UnitOfWorkTestHelper.runTestAssembly(FinalizeAssembly.class, testOutput, properties);
  }

  /**
   * Test the {@link FBisimulationProcess} process
   */
  @Test
  public void testIterationSplits()
  throws Exception {
    runFBisimulationProcess("testIterationSplits");
  }

  /**
   * Test the {@link FBisimulationProcess} process
   */
  @Test
  public void testIterationSplits2()
  throws Exception {
    runFBisimulationProcess("testIterationSplits2");
  }

  /**
   * Run the {@link AdjacencyListAssembly} assembly
   * @param folder the path to the test folder
   */
  private void runAdjacencyListAssembly(final String folder)
  throws Exception {
    final String inSource = RES + "testFBisimulation/" + folder + "/input.check";
    final String outSource0 = RES + "testFBisimulation/" + folder + "/output0.check";
    final String outSource1 = RES + "testFBisimulation/" + folder + "/output1.check";
    final String outSource2 = RES + "testFBisimulation/" + folder + "/output2.check";

    properties.setProperty(TupleType.FIELD_TYPE, Hash128Type.class.getName());
    Class<?>[] typesInput = { Hash64Type.class, Hash128Type.class, EntityDescriptionType.class, EntityDescriptionType.class };
    Class<?>[] typesOutput = { Hash64Type.class, Hash128Type.class, TupleType.class, FBisimulationCidType.class };

    UnitOfWorkTestHelper.addSource(Analytics.getName(AdjacencyListAssembly.class), inSource, typesInput);
    UnitOfWorkTestHelper.addSink("intra", outSource0, typesOutput);
    UnitOfWorkTestHelper.addSink("sources", outSource1, typesOutput);
    UnitOfWorkTestHelper.addSink("sinks", outSource2, typesOutput);
    UnitOfWorkTestHelper.runTestAssembly(AdjacencyListAssembly.class, testOutput, properties);
  }

  /**
   * Run the {@link SplittingAssembly} assembly
   * @param pOld the path to the current partition of the graph
   * @param sOld the path to the current set of splited nodes
   * @param pNew the path to the new partition of the graph
   * @param sNew the path to the new set of splited nodes
   */
  private void runSplittingAssembly(final String pOld, final String sOld, final String pNew, final String sNew)
  throws Exception {
    Class<?>[] types = { Hash64Type.class, Hash128Type.class, TupleType.class, FBisimulationCidType.class };

    properties.setProperty(TupleType.FIELD_TYPE, Hash128Type.class.getName());
    UnitOfWorkTestHelper.addSource("partition-old", pOld, types);
    UnitOfWorkTestHelper.addSource("splits-old", sOld, types);
    UnitOfWorkTestHelper.addSink("partition-new", pNew, types);
    UnitOfWorkTestHelper.addSink("splits-new", sNew, types);
    UnitOfWorkTestHelper.runTestAssembly(SplittingAssembly.class, testOutput, properties);
  }

  /**
   * Run the {@link FBisimulationProcess} as a {@link RiffleProcessFlow}.
   * @param folder the path to the test folder
   */
  private void runFBisimulationProcess(final String folder)
  throws Exception {
    Class<?>[] typesInput = { Hash64Type.class, Hash128Type.class, EntityDescriptionType.class,
        EntityDescriptionType.class };
    Class<?>[] typesOutput = { Hash64Type.class, Hash128Type.class, EntityDescriptionType.class,
        EntityDescriptionType.class, FBisimulationCidType.class };

    final String input = RES + "testFBisimulation/" + folder + "/input.check";
    final String output = RES + "testFBisimulation/" + folder + "/output.check";

    UnitOfWorkTestHelper.addSource(Analytics.getName(FBisimulationProcess.class), input, typesInput);
    UnitOfWorkTestHelper.addSink(Analytics.getName(FBisimulationProcess.class), output, typesOutput);
    UnitOfWorkTestHelper.runTestAssembly(FBisimulationProcess.class, testOutput, properties);
  }

}
