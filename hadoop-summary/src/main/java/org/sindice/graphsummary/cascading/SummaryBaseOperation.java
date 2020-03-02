/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Set;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
import org.sindice.core.analytics.rdf.AnalyticsClassAttributes;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * This {@link BaseOperation} sets parameters for the summary computation.
 * @see AnalyticsParameters
 * @see SummaryParameters
 */
public abstract class SummaryBaseOperation<Context>
extends BaseOperation<Context> {

  private static final long serialVersionUID = 8320997208158571248L;

  /**
   * Constructs a new instance that returns an {@link Fields#UNKNOWN} {@link Tuple} and accepts any number of arguments.
   * </p> It is a best practice to always declare the field names and number of arguments via one of the other
   * constructors.
   */
  protected SummaryBaseOperation() {
    super();
  }

  /**
   * Constructs a new instance that returns the fields declared in fieldDeclaration and accepts any number of arguments.
   * 
   * @param fieldDeclaration
   *          of type Fields
   */
  protected SummaryBaseOperation(Fields fieldDeclaration) {
    super(fieldDeclaration);
  }

  /**
   * Constructs a new instance that returns an unknown field set and accepts the given numArgs number of arguments.
   * 
   * @param numArgs
   *          of type numArgs
   */
  protected SummaryBaseOperation(int numArgs) {
    super(numArgs);
  }

  /**
   * Constructs a new instance that returns the fields declared in fieldDeclaration and accepts numArgs number of
   * arguments.
   * 
   * @param numArgs
   *          of type numArgs
   * @param fieldDeclaration
   *          of type Fields
   */
  protected SummaryBaseOperation(int numArgs, Fields fieldDeclaration) {
    super(numArgs, fieldDeclaration);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall) {
    // Class Attributes
    final String ca = flowProcess.getStringProperty(AnalyticsParameters.CLASS_ATTRIBUTES_FIELD.toString());
    final String[] classAttributes = StringUtils.getStrings(ca);
    AnalyticsClassAttributes.initClassAttributes(classAttributes == null ? AnalyticsParameters.CLASS_ATTRIBUTES_FIELD
    .get() : classAttributes);

    // Normalise literal types
    final String norm = flowProcess.getStringProperty(AnalyticsParameters.NORM_LITERAL_TYPE.toString());
    AnalyticsParameters.NORM_LITERAL_TYPE.set(Boolean.parseBoolean(norm));

    // Cluster Authority Check
    final String check = flowProcess.getStringProperty(AnalyticsParameters.CHECK_AUTH_TYPE.toString());
    if (check != null) {
      AnalyticsParameters.CHECK_AUTH_TYPE.set(Boolean.parseBoolean(check));
    }

    // Datatype
    final String datatype = flowProcess.getStringProperty(SummaryParameters.DATATYPE.toString());
    if (datatype != null) {
      SummaryParameters.DATATYPE.set(Boolean.parseBoolean(datatype));
    }

    // Predicates black list
    setBlackList(flowProcess, AnalyticsParameters.PREDICATES_BLACKLIST.toString(),
      AnalyticsParameters.PREDICATES_BLACKLIST.get());
    setBlackList(flowProcess, AnalyticsParameters.PREDICATES_BLACKLIST_REGEXP.toString(),
      AnalyticsParameters.PREDICATES_BLACKLIST_REGEXP.get());
  }

  private void setBlackList(final FlowProcess flowProcess, final String param, final Set<String> blacklist) {
    final String attr = flowProcess.getStringProperty(param);
    final String[] blackListAttributes;

    if (attr == null) { // get from the DistributedCache
      final JobConf conf = ((HadoopFlowProcess) flowProcess).getJobConf();
      final URL url = conf.getResource(param);
      if (url != null) {
        try {
          final BufferedReader r = new BufferedReader(new InputStreamReader(url.openStream()));
          String line = null;
          try {
            while ((line = r.readLine()) != null) {
              blacklist.add(line);
            }
          } finally {
            r.close();
          }
        } catch(IOException e) {
          throw new RuntimeException("Unable to get the list of black predicates", e);
        }
      }
      blackListAttributes = null;
    } else {
      blackListAttributes = StringUtils.getStrings(attr);
    }
    if (blackListAttributes != null) {
      for (String p : blackListAttributes) {
        blacklist.add(p);
      }
    }
  }

}
