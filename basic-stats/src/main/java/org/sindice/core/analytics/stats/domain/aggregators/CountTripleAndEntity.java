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

import java.util.HashSet;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * @author Diego Ceccarelli <diego.ceccarelli@deri.org>
 * @link www.di.unipi.it/~ceccarel
 */
public class CountTripleAndEntity extends BaseOperation<CountTripleAndEntity.Context>
	implements Aggregator<CountTripleAndEntity.Context> {

	private static final long serialVersionUID = 1L;
	
	public static class Context {
		Tuple result = new Tuple();
		String sndDomain;
		Long sndDomHash;
		Integer nTriple = 0;
		Integer nUniqueTriple = 0;
		Set<Long> subjects = new HashSet<Long>();
	}
	
	public CountTripleAndEntity() {
		this(new Fields("sndDomain","sndDom-hash", "subject", "nTriple","nUniqueTriple"));
	}
	
	public CountTripleAndEntity(Fields fieldDeclaration) {
		// expects 1 argument, fail otherwise
		super(1, fieldDeclaration);
	}
	
	public void start(FlowProcess flowProcess,
			AggregatorCall<Context> aggregatorCall) {
		// set the context object, starting at zero
		aggregatorCall.setContext(new Context());
	}
	
	public void aggregate(FlowProcess flowProcess,
			AggregatorCall<Context> aggregatorCall) {
		TupleEntry arguments = aggregatorCall.getArguments();
		Context context = aggregatorCall.getContext();
				
		if(context.nTriple == 0) {
			context.sndDomain = arguments.getString("sndDomain");
			context.sndDomHash = arguments.getLong("sndDom-hash");
		}
 
		context.subjects.add(arguments.getLong("subject"));
		context.nTriple += arguments.getInteger("nTriple");
		context.nUniqueTriple += arguments.getInteger("nUniqueTriple");
	
	
	}
	
	public void complete(FlowProcess flowProcess,
			AggregatorCall<Context> aggregatorCall) {
		Context context = aggregatorCall.getContext();
			
		TupleEntry arguments = aggregatorCall.getArguments();
	
		context.result.addAll(context.sndDomain, context.sndDomHash, context.subjects.size(),
				context.nTriple, context.nUniqueTriple);
		aggregatorCall.getOutputCollector().add(context.result);
		context.result.clear();
		context.subjects.clear();
	
	}
}

