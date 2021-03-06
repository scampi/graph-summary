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
 * Adds a field with the domain of the uri in object
 * 
 * @author Diego Ceccarelli <diego.ceccarelli@deri.org>
 * @link www.di.unipi.it/~ceccarel
 */

public class RDFDomainTripleFunction
extends AbstractRDFDatasetFunction {
  private static final long serialVersionUID = -5131944896505105838L;

  public RDFDomainTripleFunction() {
    super(1, new Fields("output"));
  }

  public RDFDomainTripleFunction(final Fields fieldDeclaration) {
    // expects 1 arguments, fail otherwise
    super(1, fieldDeclaration);
  }

  @Override
  public void operate(FlowProcess flowProcess,
      FunctionCall<Context> functionCall) {
    super.operate(flowProcess, functionCall);

    final TupleEntry arguments = functionCall.getArguments();
    final Context cxt = functionCall.getContext();

    // Get the fields
    final String domain = arguments.getString("domain");
    final Long nTriple = arguments.getLong("nTriple");
    final Long nDoc = arguments.getLong("nDoc");
    final Long hash = Hash.getHash64(domain);
    final URI bnode = cxt.value.createURI(DatasetsAnalyticsVocab.ANY23_PREFIX, hash.toString().replace('-', 'n'));
    final URI domainUri = cxt.value.createURI(DatasetsAnalyticsVocab.DOMAIN_URI_PREFIX + domain);
    final Literal domainLit = cxt.value.createLiteral(domain);
    final Literal nTripleLit = cxt.value.createLiteral(nTriple);
    final Literal nDocLit = cxt.value.createLiteral(nDoc);

    super.writeStatement(cxt, functionCall.getOutputCollector(), bnode,
        cxt.domainName, domainLit, cxt.graphName);
    super.writeStatement(cxt, functionCall.getOutputCollector(), bnode,
        cxt.domainUri, domainUri, cxt.graphName);
    super.writeStatement(cxt, functionCall.getOutputCollector(), bnode,
        cxt.totalExpNT, nTripleLit, cxt.graphName);
    super.writeStatement(cxt, functionCall.getOutputCollector(), bnode,
        cxt.totalDocs, nDocLit, cxt.graphName);
  }

}
