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
import java.util.LinkedList;

import org.junit.Test;
import org.sindice.core.analytics.stats.domain.assembly.CountClassPerDomain;
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
public class DomainClassTest extends AbstractAnalyticsTestCase {

  protected final static String jsonPath = "./src/test/resources/basic-input.json";
  protected final static String domain1 = "domain1.com";
  protected final static String domain2 = "domain2.com";
  protected final static String obj1 = "http://www.domain1.com/~object1";
  protected final static String obj2 = "http://www.domain1.com/~object2";

  @Test
  public void Test() throws IOException {
    Scheme sourceScheme = new TextLine(new Fields("line"));
    Tap source = new Hfs(sourceScheme, jsonPath);
    Scheme sinkScheme = new TextLine(new Fields("output"));
    Tap sink = new Hfs(sinkScheme, testOutput.getAbsolutePath(),
        SinkMode.REPLACE);

    Pipe parse = new CountClassPerDomain();

    // plan a new Flow from the assembly using the source and sink Taps
    // with the above properties
    Flow flow = new HadoopFlowConnector().connect(source, sink, parse);

    flow.complete();

    LinkedList<String> expectedTuples = new LinkedList<String>();
    expectedTuples.add(domain1 + "\t" + obj2 + "\t1\t1");
    expectedTuples.add(domain2 + "\t" + obj1 + "\t2\t2");
    expectedTuples.add(domain2 + "\t" + obj2 + "\t4\t2");

    final TupleEntryIterator iterator = flow.openSink();

    while (iterator.hasNext()) {
      TupleEntry tuple = iterator.next();
      assertTrue(expectedTuples.contains(tuple.getTuple().toString()));
      expectedTuples.remove(tuple.getTuple().toString());
    }

    // Check the correct number of tuples
    assertTrue(expectedTuples.isEmpty());
  }

}
