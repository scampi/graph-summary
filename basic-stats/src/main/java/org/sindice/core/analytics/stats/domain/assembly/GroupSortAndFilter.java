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
package org.sindice.core.analytics.stats.domain.assembly;

import org.sindice.core.analytics.cascading.operation.LessThanOrEqualsFilter;

import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.operation.filter.Limit;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

/**
 * @author Diego Ceccarelli <diego.ceccarelli@deri.org>
 * @link www.di.unipi.it/~ceccarel
 */
public class GroupSortAndFilter extends SubAssembly {

  private static final long serialVersionUID = 1L;

  private static long topN = 100000;
  private static int filterThreshold = 5;
  private static Class<? extends Number> type = Integer.class;
  private static String sortFields = null;
  private static String filterField = null;
  private static boolean descendSort = true;
  private static String[] groupFields = null;
  private static String sharedField = "";

  private static Fields fieldDeclaration;

  public GroupSortAndFilter(final String name) {
    this.setTails(assemble(new Pipe(name)));
  }

  public static void setFieldDeclaration(Fields f) {
    fieldDeclaration = f;
  }

  private Pipe assemble(Pipe... previous) {
    Pipe pipe = new Pipe("sort-assembly", previous[0]);

    // Filter results below the threshold
    pipe = new Each(pipe, new LessThanOrEqualsFilter(filterField,
        filterThreshold, type));
    // Insert pseudo field to group by and sort by sorting field
    pipe = new Each(pipe, new Insert(new Fields(sharedField), 0), Fields.ALL);
    pipe = new GroupBy(pipe, new Fields(sharedField), new Fields(sortFields),
        descendSort);
    // Group the fields by grouping field
    pipe = new GroupBy(pipe, new Fields(groupFields));

    // return only topN records
    pipe = new Each(pipe, new Limit(topN));
    // Return only desired fields, remove pseudo field
    pipe = new Each(pipe, fieldDeclaration, new Identity());
    return pipe;
  }

  public static void settings(long _topN, int _filterThreshold,
      Class<? extends Number> _type, String[] _groupFields, String _sortField,
      String _filterField, boolean _descendSort) {
    topN = _topN;
    filterThreshold = _filterThreshold;
    type = _type;
    sortFields = _sortField;
    filterField = _filterField;
    descendSort = _descendSort;

    sharedField = "universally_shared-field";
    if (_groupFields == null) {
      groupFields = new String[] { sharedField };
    } else {
      groupFields = _groupFields;
    }
  }

}
