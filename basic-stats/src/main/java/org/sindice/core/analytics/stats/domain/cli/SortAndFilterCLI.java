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
package org.sindice.core.analytics.stats.domain.cli;

import java.util.Properties;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;
import org.sindice.core.analytics.cascading.CascadeConfYAMLoader;
import org.sindice.core.analytics.stats.basic.assembly.SortAndFilter;
import org.sindice.core.analytics.stats.domain.assembly.GroupSortAndFilter;
import org.sindice.core.analytics.util.AnalyticsException;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.management.UnitOfWork;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.TextLine;
import cascading.stats.CascadingStats;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

/**
 * @project analytics
 * @author Diego Ceccarelli <diego.ceccarelli@deri.org>
 * @link www.di.unipi.it/~ceccarel
 * @copyright Copyright (C) 2011, All rights reserved.
 */
public class SortAndFilterCLI
extends AbstractAnalyticsCLI {

  private static final String ASCEND      = "ascend";
  private static final String TOPN        = "top-n";
  private static final String THRESHOLD   = "threshold";
  private static final String SORTFIELD   = "sort-field";
  private static final String FILTERFIELD = "filter-field";
  private static final String GROUPFIELDS = "group-fields";
  private static final String TYPE        = "type";
  private static final String FIELDS      = "fields";

  public static enum RecordedTime {
    REMOVE_IMPLICIT_TIME,
  }

  @Override
  protected void initializeOptionParser(final OptionParser parser) {
    parser
    .accepts(ASCEND, "Sort the tuples in ascending order, descending is default");
    parser.accepts(TOPN, "Returns only the top N records, default 100,000")
    .withRequiredArg().describedAs("long").ofType(Long.class);
    parser
    .accepts(THRESHOLD, "Filter all record with the sort field less than this value, default is 5")
    .withRequiredArg().describedAs("long").ofType(Integer.class);
    parser
    .accepts(GROUPFIELDS, "The fields to group by to sort, index starting at 1, comma seperated no spaces, leave blank to sort without grouping")
    .withRequiredArg().describedAs("String").ofType(String.class);
    parser.accepts(SORTFIELD, "The field to sort by, index starting at 1")
    .withRequiredArg().describedAs("int").ofType(Integer.class);
    parser.accepts(FILTERFIELD, "The field to filter by, index starting at 1")
    .withRequiredArg().describedAs("int").ofType(Integer.class);
    parser
    .accepts(TYPE, "Specified the primitive type of the sort field, default is int. Options are int and long")
    .withRequiredArg().describedAs("String").ofType(String.class);
    parser.accepts(FIELDS, "Specifies the number of fields").withRequiredArg()
    .describedAs("int").ofType(Integer.class);
  }

  /**
   * Executes a basic grouping and sorting algorithm for the basic and domain
   * stats. This job can be used to group the stats by fields and then sort them
   * or simply sort the records by a specified field. Users can also specify a
   * filtering threshold and the number of records allowed to be output.
   * 
   * @param options
   *          The options for the job. See options in
   *          {@link #initializeOptionParser(OptionParser)
   *          initializeOptionParser}
   * @throws AnalyticsException
   *           AnalyticsExceptoin
   */
  @Override
  protected UnitOfWork<? extends CascadingStats> doRun(final OptionSet options)
  throws Exception {
    final Properties properties = cascadeConf.getFlowsConfiguration()
    .get(CascadeConfYAMLoader.DEFAULT_PARAMETERS);

    AppProps.setApplicationJarClass(properties, SortAndFilterCLI.class);

    // Generate the input fields names
    String fields[];
    if (options.has(FIELDS)) {

      final int fieldsNumber = (Integer) options.valueOf(FIELDS);
      fields = new String[fieldsNumber];
      for (int i = 0; i < fieldsNumber; i++) {
        fields[i] = "fields-" + (i + 1);
      }
    } else {
      throw new IllegalArgumentException("Number of fields unspecified");
    }

    // Check for input directory
    final Tap<?, ?, ?> source = new Hfs(new SequenceFile(new Fields(fields)), input.get(0));

    // Check if output should be sorted in ascending or descending order
    boolean descendSort = true;
    if (options.has(ASCEND)) {
      descendSort = false;
    }

    // Check the number of records to be returned
    long topN = 1000;
    if (options.has(TOPN)) {
      topN = (Long) options.valueOf(TOPN);

      if (topN < 1) {
        topN = Long.MAX_VALUE;
      }
    }

    // Filter all records with a value below a specific threshold for the
    // filtering field
    int filterThreshold = 5;
    if (options.has(THRESHOLD)) {
      filterThreshold = (Integer) options.valueOf(THRESHOLD);
    }

    // Set the field to filter by
    String filterField = null;
    if (options.has(FILTERFIELD)) {
      int n = (Integer) options.valueOf(FILTERFIELD);

      if (n < 1 || n > fields.length) {
        throw new IllegalArgumentException("filter field is invalid");
      }

      filterField = "fields-" + n;
    } else {
      throw new IllegalArgumentException("Filter field unspecified");
    }

    // Set the field to group the data by, If set to zero, the data will not
    // be grouped, just sorted
    String[] groupFields = null;
    if (options.has(GROUPFIELDS)) {
      String s = (String) options.valueOf(GROUPFIELDS);

      groupFields = s.split(",");
      for (int i = 0; i < groupFields.length; i++) {
        int n = Integer.parseInt(groupFields[i]);
        if (n < 0 || n > fields.length) {
          throw new IllegalArgumentException("Group field is invalid");
        }

        groupFields[i] = "fields-" + n;
      }

      // If set to zero, do not group by any field, just sort
      if (groupFields.length == 1 && groupFields[0].equals("fields-0")) {
        groupFields = null;
      }

    }

    // Set field to sort records by
    String sortField = null;
    if (options.has(SORTFIELD)) {
      int n = (Integer) options.valueOf(SORTFIELD);

      if (n < 1 || n > fields.length) {
        throw new IllegalArgumentException("Sort field is invalid: " + n);
      }

      sortField = "fields-" + n;
    } else {
      throw new IllegalArgumentException("sort field unspecified");
    }

    // Set type of data that will be sorted
    Class<? extends Number> type = Integer.class;
    if (options.has(TYPE)) {
      String s = (String) options.valueOf(TYPE);
      if (s.equalsIgnoreCase("long")) {
        type = Long.class;
      } else if (s.equalsIgnoreCase("int")) {
        type = Integer.class;
      } else {
        throw new IllegalArgumentException("Invalid Type");
      }
    }

    Pipe pipe = null;

    // If the statistics should be sorted and NOT grouped first
    if (groupFields == null) {
      SortAndFilter
      .settings(topN, filterThreshold, type, groupFields, sortField, filterField, descendSort);
      SortAndFilter.setFieldDeclaration(new Fields(fields));
      pipe = new SortAndFilter("sort-n-filter");
    } else {// If the statistics should be grouped and then sorted
      GroupSortAndFilter.setFieldDeclaration(new Fields(fields));
      GroupSortAndFilter
      .settings(topN, filterThreshold, type, groupFields, sortField, filterField, descendSort);
      pipe = new GroupSortAndFilter("sort-n-filter");
    }

    final Flow<?> flow = new HadoopFlowConnector(properties)
    .connect("sort-n-filter", source, new Hfs(new TextLine(), output.get(0), SinkMode.REPLACE), pipe);

    flow.start();
    flow.complete();
    return flow;
  }

  public static void main(final String[] args)
  throws Exception {
    final SortAndFilterCLI cli = new SortAndFilterCLI();
    int res = ToolRunner.run(new Configuration(), cli, args);
    System.exit(res);
  }

}
