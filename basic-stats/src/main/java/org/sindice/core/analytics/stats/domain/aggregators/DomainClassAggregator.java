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
package org.sindice.core.analytics.stats.domain.aggregators;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Takes a group of entities with the same domain and object and produce a
 * tuples containing: < domain, subject, classes, predicates, objects>
 * 
 * @author Diego Ceccarelli <diego.ceccarelli@deri.org>
 * @link www.di.unipi.it/~ceccarel
 */
public class DomainClassAggregator
extends BaseOperation<DomainClassAggregator.Context>
implements Aggregator<DomainClassAggregator.Context> {

  private static final long serialVersionUID = 1L;

  public static class Context {
    final Tuple result = new Tuple();
    long docCount = 0;
    long totalCount = 0;
    String domain = null;
    String clazz = null;
    final Map<String, Long> predicates = new TreeMap<String, Long>();
    final StringBuilder sb = new StringBuilder();
  }

  public DomainClassAggregator(Fields fieldDeclaration) {
    // expects 1 argument, fail otherwise
    super(1, fieldDeclaration);
  }

  public void start(FlowProcess flowProcess,
      AggregatorCall<Context> aggregatorCall) {
    if (aggregatorCall.getContext() == null) {
      aggregatorCall.setContext(new Context());
    } else {
      final Context c = aggregatorCall.getContext();
      c.predicates.clear();
      c.result.clear();
      c.docCount = 0;
      c.totalCount = 0;
      c.domain = null;
      c.clazz = null;
    }
  }

  public void aggregate(FlowProcess flowProcess,
      AggregatorCall<Context> aggregatorCall) {

    TupleEntry arguments = aggregatorCall.getArguments();
    Context context = aggregatorCall.getContext();

    long totalCount = arguments.getInteger("total-count");
    long docCount = arguments.getInteger("doc-count");
    String predicate = arguments.getString("predicate");
    String domain = arguments.getString("sndDomain");
    String clazz = arguments.getString("class");

    context.predicates.put(predicate, docCount);
    context.domain = domain;
    context.clazz = clazz;
    context.docCount += docCount;
    context.totalCount += totalCount;

  }

  public void complete(FlowProcess flowProcess,
      AggregatorCall<Context> aggregatorCall) {
    Context context = aggregatorCall.getContext();

    context.sb.setLength(0);

    for (Entry<String, Long> t : context.predicates.entrySet()) {
      String pred = t.getKey();
      Long cnt = t.getValue().longValue();
      context.sb.append(pred).append(" ").append(cnt).append("\t");
    }

    String types = context.sb.toString();
    context.result.addAll(context.domain, context.clazz, context.totalCount,
        context.docCount, types);
    aggregatorCall.getOutputCollector().add(context.result);

    context.result.clear();
    context.predicates.clear();
    context.sb.setLength(0);

  }
}
