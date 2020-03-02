/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.hash2value.rdf;

import java.io.IOException;

import org.apache.hadoop.hbase.io.hfile.AnalyticsCacheConfig;
import org.apache.hadoop.util.StringUtils;
import org.openrdf.model.URI;
import org.sindice.analytics.entity.AnalyticsValue;
import org.sindice.core.analytics.cascading.AbstractAnalytics2RDFOperation;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
import org.sindice.core.analytics.cascading.dictionary.Dictionary;
import org.sindice.core.analytics.cascading.dictionary.DictionaryFactory;
import org.sindice.core.analytics.rdf.AnalyticsClassAttributes;
import org.sindice.core.analytics.rdf.SummaryContext;
import org.sindice.graphsummary.cascading.hash2value.GenerateDictionary;

import cascading.flow.FlowProcess;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;

/**
 * Base class for mapping hashed values back to string, and for the RDFication of the summary.
 */
public abstract class AbstractDGSRDFExportOperation<C extends AbstractDGSRDFExportOperation.Context>
extends AbstractAnalytics2RDFOperation<C> {

  private static final long  serialVersionUID  = 9038825202589427455L;

  public static final String DOMAIN_URI_PREFIX = "domain-uri-prefix";

  /** @see AnalyticsParameters#DATASET_URI */
  protected URI              datasetUri        = null;

  /** @see AnalyticsParameters#CONTEXT */
  private SummaryContext     context;

  protected class Context
  extends AbstractAnalytics2RDFOperation<Context>.Context {
    // for the RDFication
    final URI        pLabel           = value.createURI(DataGraphSummaryVocab.LABEL.toString());
    final URI        pCard            = value.createURI(DataGraphSummaryVocab.CARDINALITY.toString());
    final URI        pType            = value.createURI(DataGraphSummaryVocab.TYPE.toString());
    final URI        sindiceGraphName = value.createURI(DataGraphSummaryVocab.GRAPH_SUMMARY_GRAPH);
    final URI        pEdgeSrc         = value.createURI(DataGraphSummaryVocab.EDGE_SOURCE.toString());
    final URI        pEdgeDst         = value.createURI(DataGraphSummaryVocab.EDGE_TARGET.toString());
    final URI        pEdgePublishedIn = value.createURI(DataGraphSummaryVocab.EDGE_PUBLISHED_IN.toString());
    final URI        pEdgeDatatype    = value.createURI(DataGraphSummaryVocab.EDGE_DATATYPE.toString());

    final URI        pDomain          = value.createURI(DataGraphSummaryVocab.DOMAIN_URI.toString());
    final URI        pDomainName      = value.createURI(DataGraphSummaryVocab.DOMAIN_NAME.toString());
    final URI        pGlobalID        = value.createURI(DataGraphSummaryVocab.GLOBAL_ID.toString());

    // for mapping the values back to string
    final Dictionary domainDict;
    final Dictionary predicateDict;
    final Dictionary classDict;
    final Dictionary datatypeDict;

    public Context(FlowProcess flowProcess) {
      // Set up the dictionaries
      try {
        domainDict = DictionaryFactory.getDictionary(flowProcess, GenerateDictionary.DOMAIN_PREFIX);
        logger.info("Loaded Domain dictionary");
        predicateDict = DictionaryFactory.getDictionary(flowProcess, GenerateDictionary.PREDICATE_PREFIX);
        logger.info("Loaded Predicate dictionary");
        classDict = DictionaryFactory.getDictionary(flowProcess, GenerateDictionary.CLASS_PREFIX);
        logger.info("Loaded Class dictionary");
        datatypeDict = DictionaryFactory.getDictionary(flowProcess, GenerateDictionary.DATATYPE_PREFIX);
        logger.info("Loaded Datatype dictionary");
      } catch (Exception e) {
        logger.error("Couldn't load dictionary: {}", e);
        throw new IllegalArgumentException("Unable to load HFile", e);
      }
    }

  }

  public AbstractDGSRDFExportOperation(int numArgs, Fields fieldDeclaration) {
    super(numArgs, fieldDeclaration);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<C> operationCall) {
    super.prepare(flowProcess, operationCall);

    // Class Attributes
    final String ca = flowProcess.getStringProperty(AnalyticsParameters.CLASS_ATTRIBUTES_FIELD.toString());
    final String[] classAttrs = StringUtils.getStrings(ca);
    AnalyticsClassAttributes.initClassAttributes(classAttrs == null ? AnalyticsParameters.CLASS_ATTRIBUTES_FIELD.get()
                                                                    : classAttrs);

    final String prefix = flowProcess.getStringProperty(DOMAIN_URI_PREFIX);
    DataGraphSummaryVocab.setDomainUriPrefix(prefix == null ? DataGraphSummaryVocab.DOMAIN_URI_PREFIX
                                                            : prefix);

    final String du = flowProcess.getStringProperty(AnalyticsParameters.DATASET_URI.toString());
    if (du != null) {
      datasetUri = operationCall.getContext().value.createURI(du);
    }

    final String c = flowProcess.getStringProperty(AnalyticsParameters.CONTEXT.toString());
    context = c == null ? AnalyticsParameters.CONTEXT.get() : SummaryContext.valueOf(c);
  }

  @Override
  protected C getContext(FlowProcess<?> flowProcess) {
    return (C) new Context(flowProcess);
  }

  /**
   * Returns the context {@link URI} depending on the parameter {@link AnalyticsParameters#CONTEXT}
   * @param c the {@link Context} instance
   * @param domain the dataset label
   */
  protected URI getContext(final C c,
                           final String domain) {
    switch (context) {
      case DATASET:
        return c.value.createURI(DataGraphSummaryVocab.DOMAIN_URI_PREFIX, domain);
      case SINDICE:
        return c.sindiceGraphName;
      default:
        throw new EnumConstantNotPresentException(SummaryContext.class, context.toString());
    }
  }

  /**
   * Closes the dictionaries
   */
  @Override
  public void cleanup(FlowProcess flowProcess,
                      OperationCall<C> operationCall) {
    final C c = operationCall.getContext();

    if (c == null) {
      return;
    }
    try {
      closeDictionary(c.domainDict);
    } finally {
      try {
        closeDictionary(c.predicateDict);
      } finally {
        closeDictionary(c.classDict);
      }
      AnalyticsCacheConfig.unRegisterAll();
    }
  }

  /**
   * Closes the {@link Dictionary} and logs the {@link IOException}.
   */
  private void closeDictionary(Dictionary dict) {
    try {
      if (dict != null) {
        dict.close();
      }
    } catch(IOException e) {
      logger.error("Failed to close dictionary", e);
    }
  }

  /**
   * Retrieve from the dictionary the {@link AnalyticsValue} associated with the long value.
   * Returns <code>null</code> if the value is not in the dictionary.
   * @param cache the {@link Dictionary} to search in
   * @param value the key value to search for
   * @return the associated {@link AnalyticsValue}
   */
  protected AnalyticsValue decodeLongValue(final Dictionary cache,
                                           final long value) {
    return (AnalyticsValue) cache.getValue(value);
  }

}
