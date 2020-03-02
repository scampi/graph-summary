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
public abstract class AbstractRDFDatasetFunction
extends AbstractAnalytics2RDFOperation<AbstractRDFDatasetFunction.Context>
implements Function<AbstractRDFDatasetFunction.Context> {

  private static final long serialVersionUID = -8732077148146496856L;

  public static final String DATE = "date";

  public class Context extends AbstractAnalytics2RDFOperation<Context>.Context {

    final URI statisticsName = value.createURI(DatasetsAnalyticsVocab.STATISTIC_NAME
        .toString());
    final URI domainName = value.createURI(DatasetsAnalyticsVocab.DOMAIN_NAME
        .toString());
    final URI domainUri = value
        .createURI(DatasetsAnalyticsVocab.DOMAIN_URI.toString());
    final URI label = value.createURI(DatasetsAnalyticsVocab.LABEL.toString());
    final URI cardinality = value.createURI(DatasetsAnalyticsVocab.CARDINALITY
        .toString());
    final URI documentCard = value.createURI(DatasetsAnalyticsVocab.DOCUMENT_CARD
        .toString());

    final URI domainClassStat = value
        .createURI(DatasetsAnalyticsVocab.DOMAIN_CLASS_STAT.toString());
    final URI totalEntities = value.createURI(DatasetsAnalyticsVocab.TOTAL_ENTITIES
        .toString());

    final URI domainNSStat = value
        .createURI(DatasetsAnalyticsVocab.DOMAIN_NAMESPACE_STAT.toString());

    final URI domainPredStat = value
        .createURI(DatasetsAnalyticsVocab.DOMAIN_PREDICATE_STAT.toString());

    final URI patternBUB = value.createURI(DatasetsAnalyticsVocab.PATTERN_BUB
        .toString());
    final URI patternBUL = value.createURI(DatasetsAnalyticsVocab.PATTERN_BUL
        .toString());
    final URI patternBUU = value.createURI(DatasetsAnalyticsVocab.PATTERN_BUU
        .toString());
    final URI patternUUB = value.createURI(DatasetsAnalyticsVocab.PATTERN_UUB
        .toString());
    final URI patternUUL = value.createURI(DatasetsAnalyticsVocab.PATTERN_UUL
        .toString());
    final URI patternUUU = value.createURI(DatasetsAnalyticsVocab.PATTERN_UUU
        .toString());

    final URI totalExpNT = value
        .createURI(DatasetsAnalyticsVocab.TOTAL_EXPLICIT_TRIPLES.toString());
    final URI totalDocs = value.createURI(DatasetsAnalyticsVocab.TOTAL_DOCUMENTS
        .toString());

    final URI graphName = value
        .createURI(DatasetsAnalyticsVocab.DOMAIN_ANALYTICS_GRAPH);
    boolean date = true;
    String dateString;
  }

  public AbstractRDFDatasetFunction(int numArgs, Fields fieldDeclaration) {
    super(numArgs, fieldDeclaration);
  }

  @Override
  protected Context getContext(FlowProcess flowProcess) {
    return new AbstractRDFDatasetFunction.Context();
  }

  @Override
  public void prepare(FlowProcess flowProcess,
                      OperationCall<AbstractRDFDatasetFunction.Context> operationCall) {
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
    c.dateString = conf.get(DATE);
  }

  @Override
  public void operate(FlowProcess flowProcess,
                      FunctionCall<AbstractRDFDatasetFunction.Context> functionCall) {
    final Context context = functionCall.getContext();

    if (context.date) {
      context.date = false;
      final URI datePred = context.value
          .createURI(DatasetsAnalyticsVocab.DATE_PREDICATE);
      final URI xsdDate = context.value.createURI(DatasetsAnalyticsVocab.XSD_DATE);
      final Literal dateLiteral = context.value.createLiteral(context.dateString, xsdDate);
      super.writeStatement(context, functionCall.getOutputCollector(),
          context.graphName, datePred, dateLiteral, context.graphName);
    }
  }

}
