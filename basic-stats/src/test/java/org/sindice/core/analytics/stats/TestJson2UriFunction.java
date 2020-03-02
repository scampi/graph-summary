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

import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.sindice.core.analytics.stats.assembly.Json2UriFunction;
import org.sindice.core.analytics.testHelper.AbstractAnalyticsTestCase;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Function;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.scheme.hadoop.WritableSequenceFile;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

/**
 * @author Diego Ceccarelli <diego.ceccarelli@isti.cnr.it>
 */
public class TestJson2UriFunction extends AbstractAnalyticsTestCase {

	private final static String jsonPath = "./src/test/resources/json-sequencefile/part-00003";

  @Test
  public void testJsonExplicitTriple() throws IOException {
    Scheme sourceScheme = new WritableSequenceFile(new Fields("value"),
        Text.class);
    Tap source = new Hfs(sourceScheme, jsonPath);

    Scheme sinkScheme = new TextLine();
    Tap sink = new Hfs(sinkScheme, testOutput.getAbsolutePath(),
        SinkMode.REPLACE);

    /* takes a json file and produces the triples */
    final Function f = new Json2UriFunction();
    Pipe p = new Pipe("test");
    p = new Each(p, new Fields("value"), f, Fields.RESULTS);
    // initialize app properties, tell Hadoop which jar file to use
    // plan a new Flow from the assembly using the source and sink Taps
    // with the above properties
    Flow flow = new HadoopFlowConnector().connect("out", source, sink, p);
    flow.complete();
  }

}
