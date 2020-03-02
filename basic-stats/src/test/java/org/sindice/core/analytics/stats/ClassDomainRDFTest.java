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

import org.junit.Ignore;
import org.junit.Test;
import org.sindice.core.analytics.stats.domain.assembly.CountClassPerDomain;
import org.sindice.core.analytics.stats.domain.rdf.AbstractRDFDatasetFunction;
import org.sindice.core.analytics.stats.domain.rdf.DatasetsAnalyticsVocab;
import org.sindice.core.analytics.stats.domain.rdf.RDFDomainClassStats;
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
@Ignore("To refactor with Arthur's test framework")
public class ClassDomainRDFTest
extends AbstractAnalyticsTestCase {

  protected final static String jsonPath         = "./src/test/resources/graph-input3.json";
  protected final static String date             = "2011-01-01";
  protected final static String domain_uri       = "<http://sindice.com/analytics/domain>";
  protected final static String date_uri         = "\"" + date +
                                                   "\"^^<http://www.w3.org/2001/XMLSchema#date>";
  protected final static String date_pred        = "<http://purl.org/dc/elements/1.1/date>";
  protected final static String stat_name_uri    = "<" +
                                                   DatasetsAnalyticsVocab.STATISTIC_NAME
                                                   .toString() + ">";

  protected final static String domain_class_uri = "<" +
                                                   DatasetsAnalyticsVocab.DOMAIN_CLASS_STAT
                                                   .toString() + ">";
  protected final static String movie            = "\"movie\"";

  @Test
  public void Test()
  throws IOException {
    Scheme sourceScheme = new TextLine(new Fields("value"));
    Tap source = new Hfs(sourceScheme, jsonPath);
    Scheme sinkScheme = new TextLine(new Fields("output"));
    Tap sink = new Hfs(sinkScheme, testOutput.getAbsolutePath(), SinkMode.REPLACE);

    Pipe parse = new CountClassPerDomain();
    parse = new RDFDomainClassStats(Pipe.pipes(new Pipe("ParseHashGraph", parse)));

    properties.setProperty(AbstractRDFDatasetFunction.DATE, date);
    Flow flow = new HadoopFlowConnector(properties).connect(source, sink, parse);
    flow.complete();

    // Note: this test assumes the tuples are produced in the following order
    ArrayList<String> expectedTuples = new ArrayList<String>();

    final TupleEntryIterator iterator = flow.openSink();
    int nTuple = 0;
    while (iterator.hasNext()) {
      TupleEntry tuple = iterator.next();

      // Note url is expected to a long while others are ints
      // assertEquals(Long.parseLong(fields[0]),
      // expectedTuples.get(nTuple).dom);
      nTuple++;
    }
    // Check the correct number of tuples
    assertEquals(expectedTuples.size(), nTuple);
  }

}
