/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.properties;

import static org.sindice.core.analytics.util.AnalyticsCounters.TIME;
import static org.sindice.graphsummary.cascading.InstanceCounters.CLASSES;
import static org.sindice.graphsummary.cascading.InstanceCounters.INSTANCE_ID;
import static org.sindice.graphsummary.cascading.InstanceCounters.PROPERTIES;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;
import org.sindice.analytics.entity.AnalyticsLiteral;
import org.sindice.analytics.entity.EntityDescription;
import org.sindice.analytics.entity.EntityDescription.Statements;
import org.sindice.core.analytics.rdf.AnalyticsClassAttributes;
import org.sindice.core.analytics.util.BytesRef;
import org.sindice.core.analytics.util.Hash;
import org.sindice.graphsummary.cascading.SummaryBaseOperation;
import org.sindice.graphsummary.cascading.SummaryParameters;
import org.sindice.graphsummary.cascading.ecluster.ClusteringFactory.ClusteringAlgorithm;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * This {@link Function} is used in conjunction with the {@link ClusteringAlgorithm#SINGLE_TYPE} and
 * filters the type which label is not the same as the one that defines the cluster. 
 */
public class SingleTypeSchemaFunction
extends SummaryBaseOperation<Tuple>
implements Function<Tuple> {

  private static final long serialVersionUID = 737253267461904049L;

  /**
   * The declared {@link Fields} are expected to have 4 elements:
   * <ul>
   * <li><b>flag</b>: the flag for the tuple, e.g., {@link GetPropertiesGraph#TYPE};</li>
   * <li><b>label</b>: the label as {@link Long} of the schema information, either a type or an attribute;</li>
   * <li><b>value</b>: the statistic related to this label; and</li>
   * <li><b>entity</b>: the number of entities. Here, the input {@link Tuple} is 1 entity, so the sum
   * of such values is equal to 1.</li>
   * </ul>
   * @param fieldDeclaration the output {@link Fields}
   */
  public SingleTypeSchemaFunction(final Fields fieldDeclaration) {
    super(2, fieldDeclaration);
  }

  @Override
  public void prepare(FlowProcess flowProcess,
                      OperationCall<Tuple> operationCall) {
    super.prepare(flowProcess, operationCall);
    operationCall.setContext(Tuple.size(4));
  }

  @Override
  public void operate(FlowProcess flowProcess,
                      FunctionCall<Tuple> functionCall) {
    final long start = System.currentTimeMillis();

    final Tuple tuple = functionCall.getContext();
    final TupleEntry args = functionCall.getArguments();

    final BytesWritable cid = (BytesWritable) args.getObject("cluster-id");

    // count for entities
    tuple.set(0, GetPropertiesGraph.ENTITY);
    tuple.set(1, null);
    tuple.set(2, null);
    tuple.set(3, 1);
    functionCall.getOutputCollector().add(tuple);

    final EntityDescription eout = (EntityDescription) args.getObject("spo-out");
    eout.setFlowProcess(flowProcess);
    final Statements spos = eout.iterateStatements();
    while (spos.getNext()) {
      if (AnalyticsClassAttributes.isClass(spos.getPredicate())) { // a type
        final BytesWritable typeHash = spos.getObjectHash128();
        if (WritableComparator.compareBytes(cid.getBytes(), 0, cid.getLength(),
          typeHash.getBytes(), 0, typeHash.getLength()) == 0) {
          flowProcess.increment(INSTANCE_ID, CLASSES, 1);
          final long objectHash = spos.getObjectHash64();
          tuple.set(0, GetPropertiesGraph.TYPE);
          tuple.set(1, objectHash);
          tuple.set(2, AnalyticsClassAttributes.CLASS_ATTRIBUTES.indexOf(spos.getPredicate()));
          tuple.set(3, 0);
          functionCall.getOutputCollector().add(tuple);
        }
      } else { // an attribute
        final long predicateHash = spos.getPredicateHash64();
        flowProcess.increment(INSTANCE_ID, PROPERTIES, 1);
        tuple.set(0, GetPropertiesGraph.ATTRIBUTE);
        tuple.set(1, predicateHash);
        if (SummaryParameters.DATATYPE.get() && spos.getObject() instanceof AnalyticsLiteral) {
          final AnalyticsLiteral lit = (AnalyticsLiteral) spos.getObject();
          final BytesRef datatype = lit.getDatatypeAsAnalyticsURI().getValue();
          if (datatype.length == 0) { // no datatype
            tuple.set(2, null);
          } else {
            tuple.set(2, Hash.getHash64(datatype));
          }
        } else {
          tuple.set(2, null);
        }
        tuple.set(3, 0);
        functionCall.getOutputCollector().add(tuple);
      }
    }

    flowProcess.increment(JOB_ID, TIME + this.getClass().getSimpleName(), System.currentTimeMillis() - start);
  }

}
