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
package org.sindice.core.analytics.stats;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.Test;
import org.sindice.core.analytics.stats.distribution.assembly.DistributionTermOverPredicate;
import org.sindice.core.analytics.testHelper.AbstractAnalyticsTestCase;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

/**
 * @project analytics
 * @author Thomas Perry <thomas.perry@deri.org>
 */
public class DistributionTermPredicateTest
extends AbstractAnalyticsTestCase {

  protected final static String jsonPath = "./src/test/resources/term-predicate-input.json";

  @Test
  public void Test()
  throws IOException {
    Scheme sourceScheme = new TextLine(new Fields("line"));
    Tap source = new Hfs(sourceScheme, jsonPath);
    Scheme sinkScheme = new TextLine(new Fields("output"));
    Tap sink = new Hfs(sinkScheme, testOutput.getAbsolutePath(), SinkMode.REPLACE);

    Pipe parse = new DistributionTermOverPredicate();

    // plan a new Flow from the assembly using the source and sink Taps
    // with the above properties
    Flow flow = new HadoopFlowConnector().connect(source, sink, parse);

    flow.start();
    flow.complete();

    // Note: this test assumes the tuples are produced in the following
    // order
    ArrayList<String> expectedTuples = new ArrayList<String>();
    expectedTuples.add("1\t2\t1");

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

}
