/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.hash2value.rdf;

import static org.sindice.core.analytics.util.AnalyticsCounters.ERROR;
import static org.sindice.core.analytics.util.AnalyticsCounters.TIME;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.sindice.analytics.entity.AnalyticsValue;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 *  
 */
@SuppressWarnings("rawtypes")
public class RDFRelationsGraphFunction
extends AbstractDGSRDFExportOperation<AbstractDGSRDFExportOperation.Context>
implements Function<AbstractDGSRDFExportOperation.Context> {

  private static final long serialVersionUID = -5667289305504343724L;

  public RDFRelationsGraphFunction(Fields fieldDeclaration) {
    super(7, fieldDeclaration);
  }

  @Override
  public void operate(FlowProcess flowProcess,
                      FunctionCall<AbstractDGSRDFExportOperation.Context> call) {
    final AbstractDGSRDFExportOperation.Context c = call.getContext();
    final long start = System.currentTimeMillis();

    final TupleEntry args = call.getArguments();

    final long docHash = args.getLong(0);
    final long srcDomain = args.getLong(1);
    final String srcID = args.getString(2);
    final long pHash = args.getLong(3);
    final long dstDomain = args.getLong(4);
    final String dstID = args.getString(5);
    final long cardinality = args.getLong(6);

    final String srcHash = Long.toString(srcDomain).concat(srcID).replace('-', 'n').replace(" ", "");
    final URI src = c.value.createURI(DataGraphSummaryVocab.ANY23_PREFIX, "node" + srcHash);

    final String dstHash = Long.toString(dstDomain).concat(dstID).replace('-', 'n').replace(" ", "");
    final URI dst = c.value.createURI(DataGraphSummaryVocab.ANY23_PREFIX, "node" + dstHash);

    /*
     * Edge
     */
    final AnalyticsValue predicate = decodeLongValue(c.predicateDict, pHash);
    if (predicate == null) {
      logger.error("Predicate not found, removing entry: {}", args.toString());
      flowProcess.increment(JOB_ID, ERROR + " string value of hashed relation not found", 1);
      return;
    }
    final AnalyticsValue d = decodeLongValue(c.domainDict, docHash);
    if (d == null) {
      logger.error("Doc domain not found, removing entry: {}", args.toString());
      flowProcess.increment(JOB_ID, ERROR + " string value of hashed doc-domain not found", 1);
      return;
    }
    final String docDomain = d.stringValue();

    c.sb.setLength(0);
    final String edgeID = c.sb.append("edge").append(pHash)
                              .append("/prov").append(docHash)
                              .append("/source").append(srcHash)
                              .append("/target").append(dstHash)
                              .toString();

    final URI edge = c.value.createURI(DataGraphSummaryVocab.ANY23_PREFIX, edgeID.replace('-', 'n').replace(" ", ""));
    // Published in
    if (datasetUri == null) {
      final URI publishedIn = c.value.createURI(DataGraphSummaryVocab.DOMAIN_URI_PREFIX, docDomain);
      writeStatement(c, call.getOutputCollector(), edge, c.pEdgePublishedIn, publishedIn, getContext(c, docDomain));
    } else {
      writeStatement(c, call.getOutputCollector(), edge, c.pEdgePublishedIn, datasetUri, getContext(c, docDomain));
    }
    // PREDICATE
    writeStatement(c, call.getOutputCollector(), edge, c.pLabel, predicate, getContext(c, docDomain));
    // FREQ
    final Literal edgeFreq = c.value.createLiteral(cardinality);
    writeStatement(c, call.getOutputCollector(), edge, c.pCard, edgeFreq, getContext(c, docDomain));
    // src - dst
    writeStatement(c, call.getOutputCollector(), edge, c.pEdgeSrc, src, getContext(c, docDomain));
    writeStatement(c, call.getOutputCollector(), edge, c.pEdgeDst, dst, getContext(c, docDomain));

    flowProcess.increment(JOB_ID, TIME + this.getClass().getSimpleName(), System.currentTimeMillis() - start);
  }

}
