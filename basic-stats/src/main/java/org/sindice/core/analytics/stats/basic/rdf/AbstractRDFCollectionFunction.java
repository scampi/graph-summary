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

import org.apache.hadoop.mapred.JobConf;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.sindice.core.analytics.cascading.AbstractAnalytics2RDFOperation;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;

/**
 * @author Diego Ceccarelli <diego.ceccarelli@deri.org>
 * @link www.di.unipi.it/~ceccarel
 */
public abstract class AbstractRDFCollectionFunction
extends AbstractAnalytics2RDFOperation<AbstractRDFCollectionFunction.Context>
implements Function<AbstractRDFCollectionFunction.Context> {

  private static final long serialVersionUID = -8732077148146496856L;

  public class Context
  extends AbstractAnalytics2RDFOperation<Context>.Context {

    final URI label         = value.createURI(CollectionAnalyticsVocab.LABEL);
    final URI statsName     = value
                            .createURI(CollectionAnalyticsVocab.STATISTIC_NAME);
    final URI colClassStat  = value
                            .createURI(CollectionAnalyticsVocab.COL_CLASS_STAT);
    final URI colPredStat   = value
                            .createURI(CollectionAnalyticsVocab.COL_PREDICATE_STAT);
    final URI colFormatStat = value
                            .createURI(CollectionAnalyticsVocab.COL_FORMAT_STAT);
    final URI colUriStat    = value
                            .createURI(CollectionAnalyticsVocab.COL_URI_STAT);
    final URI colNSStat     = value
                            .createURI(CollectionAnalyticsVocab.COL_NS_STAT);

    final URI nRefs         = value.createURI(CollectionAnalyticsVocab.N_REFS);

    final URI nUrls         = value.createURI(CollectionAnalyticsVocab.N_URLS);

    final URI nDomains      = value
                            .createURI(CollectionAnalyticsVocab.N_DOMAINS);

    final URI nSndDomains   = value
                            .createURI(CollectionAnalyticsVocab.N_SND_DOMAINS);

    final URI graphName     = value
                            .createURI(CollectionAnalyticsVocab.COL_ANALYTICS_GRAPH);
    boolean   date          = true;
    String    dateString;
  }

  public AbstractRDFCollectionFunction(int numArgs, Fields fieldDeclaration) {
    super(numArgs, fieldDeclaration);
  }

  @Override
  protected Context getContext(FlowProcess flowProcess) {
    return new AbstractRDFCollectionFunction.Context();
  }

  @Override
  public void prepare(FlowProcess flowProcess,
                      OperationCall<AbstractRDFCollectionFunction.Context> operationCall) {
    super.prepare(flowProcess, operationCall);
    final Context c;
    final JobConf conf = ((HadoopFlowProcess) flowProcess).getJobConf();

    if (operationCall.getContext() == null) {
      operationCall.setContext(new Context());
      c = operationCall.getContext();
    } else {
      c = operationCall.getContext();
      c.res.clear();
      c.sb.setLength(0);
    }
    c.dateString = conf.get("date");
  }

  @Override
  public void operate(FlowProcess flowProcess,
                      FunctionCall<AbstractRDFCollectionFunction.Context> functionCall) {
    final Context context = functionCall.getContext();

    if (context.date) {
      context.date = false;
      final URI datePred = context.value
      .createURI(CollectionAnalyticsVocab.DATE_PREDICATE);
      final URI xsdDate = context.value
      .createURI(CollectionAnalyticsVocab.XSD_DATE);
      final Literal dateLiteral = context.value
      .createLiteral(context.dateString, xsdDate);
      super
      .writeStatement(context, functionCall.getOutputCollector(), context.graphName, datePred, dateLiteral, context.graphName);
    }
  }

}
