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
package org.sindice.core.analytics.stats.basic.rdf;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.sindice.core.analytics.util.Hash;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

public class RDFCollectionUriFunction
extends AbstractRDFCollectionFunction {

  private static final long serialVersionUID = 5038737465393537157L;

  public RDFCollectionUriFunction(final Fields fieldDeclaration) {
    super(1, fieldDeclaration);
  }

  @Override
  public void operate(FlowProcess flowProcess,
                      FunctionCall<Context> functionCall) {
    super.operate(flowProcess, functionCall);

    final TupleEntry args = functionCall.getArguments();
    final Context cxt = functionCall.getContext();

    // Get the fields
    final String uri = args.getString("uri");
    final long total = args.getLong("total");
    final long graph = args.getLong("graph");
    final int domain = args.getInteger("domain-freq");
    final int sndDomain = args.getInteger("snd-level-domain-freq");

    final Long hash = Hash.getHash64(uri);
    final URI bnode = cxt.value.createURI(CollectionAnalyticsVocab.ANY23_PREFIX,
      hash.toString().replace('-', 'n'));

    final Value predicateUri = cxt.value.createURI(uri);

    final Literal objectsNumberLit = cxt.value.createLiteral(total);
    final Literal urlList = cxt.value.createLiteral(graph);
    final Literal domainLit = cxt.value.createLiteral(domain);
    final Literal sndDomainList = cxt.value.createLiteral(sndDomain);

    super.writeStatement(cxt, functionCall.getOutputCollector(), bnode, cxt.statsName, cxt.colUriStat, cxt.graphName);
    super.writeStatement(cxt, functionCall.getOutputCollector(), bnode, cxt.label, predicateUri, cxt.graphName);
    super.writeStatement(cxt, functionCall.getOutputCollector(), bnode, cxt.nRefs, objectsNumberLit, cxt.graphName);
    super.writeStatement(cxt, functionCall.getOutputCollector(), bnode, cxt.nUrls, urlList, cxt.graphName);
    super.writeStatement(cxt, functionCall.getOutputCollector(), bnode, cxt.nDomains, domainLit, cxt.graphName);
    super.writeStatement(cxt, functionCall.getOutputCollector(), bnode, cxt.nSndDomains, sndDomainList, cxt.graphName);
  }

}
