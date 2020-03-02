/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.hash2value.rdf;

import static org.sindice.core.analytics.util.AnalyticsCounters.ERROR;
import static org.sindice.core.analytics.util.AnalyticsCounters.TIME;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import java.util.Map;
import java.util.Map.Entry;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.sindice.analytics.entity.AnalyticsValue;
import org.sindice.core.analytics.rdf.AnalyticsClassAttributes;
import org.sindice.graphsummary.cascading.properties.PropertiesCount;
import org.sindice.graphsummary.cascading.properties.PropertiesCount.Iterate;
import org.sindice.graphsummary.cascading.properties.TypesCount;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * The namespace of nodes and edges in the summary is {@link DataGraphSummaryVocab#ANY23_PREFIX}.
 * The localname of subject URIs follows a specific scheme:
 * <ul>
 * <li>Hash of the domain + the cluster identifier</li>
 * <li>the localname of URIs that are 1-hop away from a resource are prefixed by that resource URI</li>
 * </ul>
 * 
 * The prefix <b>an</b> is for the namespace {@link DataGraphSummaryVocab#ANY23_PREFIX}.
 * 
 * <pre>
 * an:node{hash(domain + classes)}
 * an:node{hash(domain + classes)}/label{hash(class)}
 * an:edge{hash(predicate)}/source{hash(domain + classes)}/target{hash({@link DataGraphSummaryVocab#DUMMY_CLASS_HASH})}
 * </pre>
 * 
 * Dash '-' characters in URIs are replaced with the character 'n'.
 */
public class RDFPropertiesGraphFunction
extends AbstractDGSRDFExportOperation<AbstractDGSRDFExportOperation.Context>
implements Function<AbstractDGSRDFExportOperation.Context> {

  private static final long serialVersionUID = -6465698586206445673L;

  private Literal           dummyNode;

  public RDFPropertiesGraphFunction(Fields fieldDeclaration) {
    super(2, fieldDeclaration);
  }

  @Override
  public void prepare(FlowProcess flowProcess,
                      OperationCall<AbstractDGSRDFExportOperation.Context> operationCall) {
    super.prepare(flowProcess, operationCall);
    dummyNode = operationCall.getContext().value.createLiteral(DataGraphSummaryVocab.BLANK_NODE_COLLECTION);
  }

  /**
   * Concatenate the toString output of the list of {@link Object} into a String
   */
  private String append(final StringBuilder sb, final Object... strings) {
    sb.setLength(0);
    for (Object s : strings) {
      sb.append(s.toString());
    }
    return sb.toString();
  }

  /**
   * Outputs the statements describing the node of the graph summary.
   * @param flowProcess the {@link FlowProcess}
   * @param c the {@link Context}
   * @param call the {@link FunctionCall}
   * @param types the set of {@link TypesCount types}
   * @param domain the second-level domain name from where the original statements came from
   * @param nodeHash the hash of the root URI
   * @param cCount the count of entities in this node
   * @return the {@link URI} of the root node
   */
  private URI createNode(final FlowProcess flowProcess,
                         final RDFPropertiesGraphFunction.Context c,
                         final FunctionCall<AbstractDGSRDFExportOperation.Context> call,
                         final TypesCount types,
                         final String domain,
                         final String nodeHash,
                         final long cCount) {
    final StringBuilder sb = new StringBuilder();

    final String nodeName = "node" + nodeHash;
    final URI nodeURI = c.value.createURI(DataGraphSummaryVocab.ANY23_PREFIX, nodeName);

    // DOMAIN
    if (datasetUri == null) {
      final Literal srcDomainLit = c.value.createLiteral(domain);
      final URI srcDomainUri = c.value.createURI(DataGraphSummaryVocab.DOMAIN_URI_PREFIX, domain);
      writeStatement(c, call.getOutputCollector(), nodeURI, c.pDomain, srcDomainUri, getContext(c, domain));
      writeStatement(c, call.getOutputCollector(), nodeURI, c.pDomainName, srcDomainLit, getContext(c, domain));
    } else {
      writeStatement(c, call.getOutputCollector(), nodeURI, c.pDomain, datasetUri, getContext(c, domain));
      final Literal srcDomainLit = c.value.createLiteral(datasetUri.stringValue());
      writeStatement(c, call.getOutputCollector(), nodeURI, c.pDomainName, srcDomainLit, getContext(c, domain));
    }

    // CARDINALITY
    final Literal formatedCCount = c.value.createLiteral(cCount);
    writeStatement(c, call.getOutputCollector(), nodeURI, c.pCard, formatedCCount, getContext(c, domain));

    // NODE LABEL
    for (Entry<Long, Map<Byte, Long>> tc : types.entrySet()) {
      final AnalyticsValue type = (AnalyticsValue) c.classDict.getValue(tc.getKey());
      if (type == null) {
        logger.error("Type={} not found, skipping it", tc.getKey());
        // TODO: JUnit test
        flowProcess.increment(JOB_ID, ERROR + " string value of hashed type not found", 1);
        continue;
      }

      try {
        final String ln = append(sb, nodeName, "/label", Long.toString(tc.getKey()).replace('-', 'n'));
        final URI lNode = c.value.createURI(DataGraphSummaryVocab.ANY23_PREFIX, ln);
        writeStatement(c, call.getOutputCollector(), nodeURI, c.pLabel, lNode, getContext(c, domain));

        // label
        writeStatement(c, call.getOutputCollector(), lNode, c.pLabel, type, getContext(c, domain));

        // link "type" and class-attribute
        int cnt = 0;
        for (Entry<Byte, Long> pair : tc.getValue().entrySet()) {
          final byte attrIndex = pair.getKey();
          final Long count = pair.getValue();

          // link type
          final URI typeLabelNode = c.value.createURI(DataGraphSummaryVocab.ANY23_PREFIX, append(sb, ln, "/Type", cnt++));
          writeStatement(c, call.getOutputCollector(), lNode, c.pType, typeLabelNode, getContext(c, domain));
          // link label
          if (attrIndex < 0 || attrIndex >= AnalyticsClassAttributes.CLASS_ATTRIBUTES.size()) {
            throw new IllegalArgumentException("Expected class attribute at index " + attrIndex +
              ", but only have the following registered definitions: " + AnalyticsClassAttributes.CLASS_ATTRIBUTES);
          }
          final URI typeUri = AnalyticsClassAttributes.CLASS_ATTRIBUTES.get(attrIndex);
          writeStatement(c, call.getOutputCollector(), typeLabelNode, c.pLabel, typeUri, getContext(c, domain));
          // link cardinality
          final Literal tcLit = c.value.createLiteral(count);
          writeStatement(c, call.getOutputCollector(), typeLabelNode, c.pCard, tcLit, getContext(c, domain));
        }
      } catch (IllegalArgumentException e) {
        logger.error("Error encountered when processing the class set: {}\n{}", types, e);
      }
    }
    return nodeURI;
  }

  /**
   * Outputs the statements describing the properties associated with a cluster.
   * @param flowProcess the {@link FlowProcess}
   * @param c the {@link Context}
   * @param call the {@link FunctionCall}
   * @param properties the {@link PropertiesCount}
   * @param src the {@link URI} of the cluster
   * @param domainValue the {@link String} value of the second-level domain name
   * @param nodeHash the cluster hash value
   */
  private void writeProperties(final FlowProcess flowProcess,
                               final RDFPropertiesGraphFunction.Context c,
                               final FunctionCall<AbstractDGSRDFExportOperation.Context> call,
                               final PropertiesCount properties,
                               final URI src,
                               final String domainValue,
                               final String nodeHash) {
    final Iterate it = properties.iterate();

    while (it.getNext()) { // for each property
      final AnalyticsValue predicate = (AnalyticsValue) c.predicateDict.getValue(it.getProperty());
      if (predicate == null) {
        logger.error("Property [{}] not found, skipping it", it.getProperty());
        // TODO: add counter + JUnit test
        flowProcess.increment(JOB_ID, ERROR + " string value of hashed property not found", 1);
        continue;
      }

      try {

        c.sb.setLength(0);
        final String pHash = Long.toString(it.getProperty()).replace('-', 'n');
        final String edgeID = c.sb.append("edge").append(pHash)
                                  .append("/source").append(nodeHash)
                                  .append("/target").append(DataGraphSummaryVocab.DUMMY_CLASS_HASH)
                                  .toString();

        final URI edge = c.value.createURI(DataGraphSummaryVocab.ANY23_PREFIX, edgeID);
        // Published in
        if (datasetUri == null) {
          final URI publishedIn = c.value.createURI(DataGraphSummaryVocab.DOMAIN_URI_PREFIX, domainValue);
          writeStatement(c, call.getOutputCollector(), edge, c.pEdgePublishedIn, publishedIn, getContext(c, domainValue));
        } else {
          writeStatement(c, call.getOutputCollector(), edge, c.pEdgePublishedIn, datasetUri, getContext(c, domainValue));
        }
        // PREDICATE
        writeStatement(c, call.getOutputCollector(), edge, c.pLabel, predicate, getContext(c, domainValue));
        // src - dst
        writeStatement(c, call.getOutputCollector(), edge, c.pEdgeSrc, src, getContext(c, domainValue));
        writeStatement(c, call.getOutputCollector(), edge, c.pEdgeDst, dummyNode, getContext(c, domainValue));

        final int nbDatatypes = it.getNbOfDatatypes();
        long pCard = 0;
        for (int i = 0; i < nbDatatypes; i++) {
          pCard += it.getCount();
          if (it.getDatatype() != PropertiesCount.NO_DATATYPE) {
            c.sb.setLength(0);
            final String dtHash = Long.toString(it.getDatatype()).replace('-', 'n');
            final String dtID = c.sb.append(edgeID).append("/datatype").append(dtHash).toString();
            final URI dtUri = c.value.createURI(DataGraphSummaryVocab.ANY23_PREFIX, dtID);

            final AnalyticsValue datatype = (AnalyticsValue) c.datatypeDict.getValue(it.getDatatype());
            if (datatype == null) {
              logger.error("Datatype [{}] not found, skipping it", it.getDatatype());
              // TODO: add counter + JUnit test
              flowProcess.increment(JOB_ID, ERROR + " string value of hashed datatype not found", 1);
              continue;
            }

            final Literal cardinality = c.value.createLiteral(it.getCount());
            writeStatement(c, call.getOutputCollector(), edge, c.pEdgeDatatype, dtUri, getContext(c, domainValue));
            writeStatement(c, call.getOutputCollector(), dtUri, c.pLabel, datatype, getContext(c, domainValue));
            writeStatement(c, call.getOutputCollector(), dtUri, c.pCard, cardinality, getContext(c, domainValue));
          }
          if (i + 1 != nbDatatypes) { // go to the next datatype entry if not last
            it.getNext();
          }
        }
        // FREQ
        final Literal cardinality = c.value.createLiteral(pCard);
        writeStatement(c, call.getOutputCollector(), edge, c.pCard, cardinality, getContext(c, domainValue));
      } catch (IllegalArgumentException e) {
        logger.error("Malformated property label: [{}] in the set {}", predicate, properties);
      }
    }
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<AbstractDGSRDFExportOperation.Context> call) {
    final long start = System.currentTimeMillis();

    final Context c = call.getContext();
    final TupleEntry args = call.getArguments();

    final long domain = args.getLong("domain");
    final AnalyticsValue d = decodeLongValue(c.domainDict, domain);
    if (d == null) {
      logger.error("Domain not found, removing entry: {}", args.toString());
      flowProcess.increment(JOB_ID, TIME + this.getClass().getSimpleName(), System.currentTimeMillis() - start);
      flowProcess.increment(JOB_ID, ERROR + " string value of hashed domain not found", 1);
      return;
    }
    final String domainValue = d.stringValue();

    final String clusterId = args.getString("cluster-id").replace(" ", "");
    final TypesCount types = ((TypesCount) args.getObject("types"));
    final Long cCount = args.getLong("c-count");
    /*
     * Entity Node Collection
     */
    final String srcHash = Long.toString(domain).concat(clusterId).replace('-', 'n');
    final URI src = createNode(flowProcess, c, call, types, domainValue, srcHash, cCount);

    // Global Node ID
    final String ln = "global" + clusterId;
    final URI globalNode = c.value.createURI(DataGraphSummaryVocab.ANY23_PREFIX, ln);
    writeStatement(c, call.getOutputCollector(), src, c.pGlobalID, globalNode, getContext(c, domainValue));

    /*
     * PROPERTIES
     */
    final PropertiesCount properties = ((PropertiesCount) args.getObject("properties"));
    writeProperties(flowProcess, c, call, properties, src, domainValue, srcHash);

    flowProcess.increment(JOB_ID, TIME + this.getClass().getSimpleName(), System.currentTimeMillis() - start);
  }

}
