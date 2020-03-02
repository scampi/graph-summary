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
package org.sindice.core.analytics.stats.domain;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.Test;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.stats.domain.assembly.CountTriplesPerDomain;
import org.sindice.core.analytics.testHelper.AbstractAnalyticsTestCase;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
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
public class DomainTripleTest extends AbstractAnalyticsTestCase {

  protected final static String jsonPath = "./src/test/resources/basic-input.json";
  protected final static String domain1 = "domain1.com";
  protected final static String domain2 = "domain2.com";
  protected final static String domain3 = "domain3.com";

  @Test
  public void Test() throws IOException {
    final CountTriplesPerDomain parse = new CountTriplesPerDomain();
    Scheme sourceScheme = new TextLine(Analytics.getHeadFields(parse));
    Tap source = new Hfs(sourceScheme, jsonPath);
    Scheme sinkScheme = new TextLine(new Fields("output"));
    Tap sink = new Hfs(sinkScheme, testOutput.getAbsolutePath(),
        SinkMode.REPLACE);

    // plan a new Flow from the assembly using the source and sink Taps
    // with the above properties
    Flow flow = new HadoopFlowConnector().connect(source, sink, parse);
    flow.complete();

    // Note: this test assumes the tuples are produced in the following
    // order
    ArrayList<String> expectedTuples = new ArrayList<String>();
    expectedTuples.add(domain3 + "\t2\t1");
    expectedTuples.add(domain1 + "\t3\t1");
    expectedTuples.add(domain2 + "\t6\t2");

		final TupleEntryIterator iterator = flow.openSink();
		int nTuple = 0;

    while (iterator.hasNext()) {
      TupleEntry tuple = iterator.next();
      // System.out.println(tuple.getTuple().toString());
      assertEquals(tuple.getTuple().toString(), expectedTuples.get(nTuple));
      nTuple++;
    }

    // Check the correct number of tuples
    assertEquals(expectedTuples.size(), nTuple);
  }

}
