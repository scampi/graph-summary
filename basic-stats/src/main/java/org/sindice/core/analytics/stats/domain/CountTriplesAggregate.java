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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * 
 */
public class CountTriplesAggregate extends
    BaseOperation<CountTriplesAggregate.Context> implements
    Aggregator<CountTriplesAggregate.Context> {

  private static final long serialVersionUID = 7736742133477121123L;

  private static final Logger logger = LoggerFactory
      .getLogger(CountTriplesAggregate.class);

  public class Context {
    final Tuple result = new Tuple();
    String domain = "";
    long expTriples = 0;
    long impTriples = 0;
  }

  public CountTriplesAggregate(Fields fieldDeclaration) {
    super(3, fieldDeclaration);
  }

  @Override
  public void prepare(FlowProcess flowProcess,
      OperationCall<Context> operationCall) {
    operationCall.setContext(new Context());
  }

  public void start(FlowProcess flowProcess,
      AggregatorCall<Context> aggregatorCall) {
    final Context c = aggregatorCall.getContext();

    c.domain = "";
    c.expTriples = 0;
    c.impTriples = 0;
    c.result.clear();
  }

  public void aggregate(FlowProcess flowProcess,
      AggregatorCall<Context> aggregatorCall) {
    final TupleEntry args = aggregatorCall.getArguments();
    final Context c = aggregatorCall.getContext();

    c.domain = args.getString("domain");
    c.expTriples += args.getLong("explicit-triples");
    c.impTriples += args.getLong("implicit-triples");
  }

  public void complete(FlowProcess flowProcess,
      AggregatorCall<Context> aggregatorCall) {
    final Context context = aggregatorCall.getContext();

    context.result.addAll(context.domain, context.expTriples,
        context.impTriples);
    aggregatorCall.getOutputCollector().add(context.result);
  }

}
