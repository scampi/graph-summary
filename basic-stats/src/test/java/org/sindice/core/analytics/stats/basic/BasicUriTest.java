/*******************************************************************************
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 *
 *
 * This project is a free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This project is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this project. If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.sindice.core.analytics.stats.basic;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.Test;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.stats.basic.assembly.BasicUriStats;
import org.sindice.core.analytics.stats.basic.rdf.CollectionAnalyticsVocab;
import org.sindice.core.analytics.stats.basic.rdf.RDFCollectionUriStats;
import org.sindice.core.analytics.testHelper.AbstractAnalyticsTestCase;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

/**
 * @author Thomas Perry <thomas.perry@deri.org>
 * @copyright Copyright (C) 2011, All rights reserved.
 */
public class BasicUriTest
extends AbstractAnalyticsTestCase {

  private final static String jsonPath = "./src/test/resources/uri-input.json";

  @Test
  public void Test()
  throws IOException {
    Scheme sourceScheme = new TextLine(new Fields("value"));
    Tap source = new Hfs(sourceScheme, jsonPath);
    Scheme sinkScheme = new TextLine(new Fields("output"));
    Tap sink = new Hfs(sinkScheme, testOutput.getAbsolutePath(), SinkMode.REPLACE);

    Pipe parse = new BasicUriStats();
    Flow flow = new HadoopFlowConnector().connect(source, sink, parse);

    flow.complete();

    // Note: this test assumes the tuples are produced in the following
    // order
    ArrayList<String> expectedTuples = new ArrayList<String>();
    expectedTuples.add("http://www.domain1.com/~object2\t2\t2\t2\t1");
    expectedTuples.add("http://www.domain1.com/~sub1\t2\t1\t1\t1");
    expectedTuples.add("http://www.domain1.com/~sub2\t1\t1\t1\t1");
    expectedTuples.add("http://www.domain2.com/~sub1\t3\t3\t3\t2");
    expectedTuples.add("http://www.predicate.com/#p1\t1\t1\t1\t1");
    expectedTuples.add("http://www.predicate.com/#p2\t1\t1\t1\t1");
    expectedTuples.add("http://www.w3.org/1999/02/22-rdf-syntax-ns#type\t4\t2\t2\t1");

    final TupleEntryIterator iterator = flow.openSink();
    int nTuple = 0;

    while (iterator.hasNext()) {
      TupleEntry tuple = iterator.next();
      assertEquals(tuple.getTuple().toString(), expectedTuples.get(nTuple));
      nTuple++;
    }

    // Check the correct number of tuples
    assertEquals(expectedTuples.size(), nTuple);
  }

  @Test
  public void testRDF()
  throws Exception {
    final BasicUriStats parse = new BasicUriStats();
    final Scheme sourceScheme = new TextLine(Analytics.getHeadFields(parse));
    final Tap source = new Hfs(sourceScheme, jsonPath);
    final Scheme sinkScheme = new SequenceFile(Analytics.getTailFields(parse));
    final Tap sink = new Hfs(sinkScheme, testOutput.getAbsolutePath(), SinkMode.REPLACE);
    final Flow flow = new HadoopFlowConnector().connect(source, sink, parse);
    flow.complete();

    properties.setProperty("date", "2000-00-00");
    final RDFCollectionUriStats rdf = new RDFCollectionUriStats();
    final Scheme rdfSinkScheme = new TextLine(new Fields("rdf"));
    final Tap rdfSink = new Hfs(rdfSinkScheme, testOutput.getAbsolutePath() + "/rdf", SinkMode.REPLACE);
    final Flow rdfFlow = new HadoopFlowConnector(properties).connect(sink, rdfSink, rdf);
    rdfFlow.complete();

    final TupleEntryIterator iterator = rdfFlow.openSink();
    final ArrayList<String> data = new ArrayList<String>();
    while (iterator.hasNext()) {
      TupleEntry tuple = iterator.next();
      data.add(tuple.getString(Analytics.getTailFields(rdf)));
    }
    assertTrue(RDFHelper.contains(data, CollectionAnalyticsVocab.STATISTIC_NAME, CollectionAnalyticsVocab.COL_URI_STAT));
    assertTrue(RDFHelper.contains(data, CollectionAnalyticsVocab.LABEL, "http://www.domain1.com/~object2"));
    assertTrue(RDFHelper.contains(data, CollectionAnalyticsVocab.LABEL, "http://www.domain1.com/~sub1"));
    assertTrue(RDFHelper.contains(data, CollectionAnalyticsVocab.LABEL, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"));
    assertTrue(RDFHelper.contains(data, CollectionAnalyticsVocab.N_REFS, "4"));
  }

}
