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
package org.sindice.core.analytics.stats.domain.rdf;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.sindice.core.analytics.util.Hash;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * @project analytics
 * @author Diego Ceccarelli <diego.ceccarelli@deri.org>
 * @link www.di.unipi.it/~ceccarel
 * @copyright Copyright (C) 2011, All rights reserved.
 */
public class RDFDomainPatternFunction
extends AbstractRDFDatasetFunction {

  private static final long serialVersionUID = -8732077148146496856L;

  public RDFDomainPatternFunction(final Fields fieldDeclaration) {
    super(1, fieldDeclaration);
  }

  @Override
  public void operate(FlowProcess flowProcess,
                      FunctionCall<RDFDomainPatternFunction.Context> functionCall) {
    super.operate(flowProcess, functionCall);

    // get the arguments TupleEntry
    final TupleEntry arguments = functionCall.getArguments();
    final Context cxt = functionCall.getContext();

    // Get the fields
    final String domain = arguments.getString("domain");

    final Long nUU = arguments.getLong("nUU");
    final Long nUB = arguments.getLong("nUB");
    final Long nUL = arguments.getLong("nUL");
    final Long nBU = arguments.getLong("nBU");
    final Long nBB = arguments.getLong("nBB");
    final Long nBL = arguments.getLong("nBL");

    final Long hash = Hash.getHash64(domain);
    final URI bnode = cxt.value.createURI(DatasetsAnalyticsVocab.ANY23_PREFIX,
        hash.toString().replace('-', 'n'));
    final URI domainUri = cxt.value
        .createURI(DatasetsAnalyticsVocab.DOMAIN_URI_PREFIX + domain);
    final Literal domainLit = cxt.value.createLiteral(domain);
    final Literal nUULit = cxt.value.createLiteral(nUU);
    final Literal nUBLit = cxt.value.createLiteral(nUB);
    final Literal nULLit = cxt.value.createLiteral(nUL);
    final Literal nBULit = cxt.value.createLiteral(nBU);
    final Literal nBBLit = cxt.value.createLiteral(nBB);
    final Literal nBLLit = cxt.value.createLiteral(nBL);

    super.writeStatement(cxt, functionCall.getOutputCollector(), bnode, cxt.domainName, domainLit, cxt.graphName);
    super.writeStatement(cxt, functionCall.getOutputCollector(), bnode, cxt.domainUri, domainUri, cxt.graphName);
    super.writeStatement(cxt, functionCall.getOutputCollector(), bnode, cxt.patternUUU, nUULit, cxt.graphName);
    super.writeStatement(cxt, functionCall.getOutputCollector(), bnode, cxt.patternUUB, nUBLit, cxt.graphName);
    super.writeStatement(cxt, functionCall.getOutputCollector(), bnode, cxt.patternUUL, nULLit, cxt.graphName);
    super.writeStatement(cxt, functionCall.getOutputCollector(), bnode, cxt.patternBUU, nBULit, cxt.graphName);
    super.writeStatement(cxt, functionCall.getOutputCollector(), bnode, cxt.patternBUB, nBBLit, cxt.graphName);
    super.writeStatement(cxt, functionCall.getOutputCollector(), bnode, cxt.patternBUL, nBLLit, cxt.graphName);
  }

}
