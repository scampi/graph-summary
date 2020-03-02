/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.statistics;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.codehaus.jackson.map.ObjectMapper;
import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.sindice.analytics.entity.EntityDescription;
import org.sindice.analytics.entity.EntityDescription.Statements;
import org.sindice.core.analytics.rdf.AnalyticsClassAttributes;
import org.sindice.core.analytics.rdf.RDFParser;
import org.sindice.graphsummary.cascading.SummaryBaseOperation;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * This {@link Function} computes the statistics described in {@link EntityDistribution}.
 */
public class EntityDistributionFunction
extends SummaryBaseOperation<EntityDistributionFunction.Context>
implements Function<EntityDistributionFunction.Context> {

  private static final long      serialVersionUID = 7295446156248898754L;
  private transient final String NB_TRIPLES       = "triples";
  private transient final String OBJECTS          = "objects";

  class Context {
    final Tuple               tuple   = Tuple.size(1);

    final ObjectMapper        json    = new ObjectMapper();
    final Map<String, Object> stats   = new TreeMap<String, Object>();
    final Map<String, Long>   objects = new HashMap<String, Long>();
  }

  public EntityDistributionFunction(final Fields fields) {
    super(4, fields);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall) {
    super.prepare(flowProcess, operationCall);
    operationCall.setContext(new Context());
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Context> functionCall) {
    final Context c = functionCall.getContext();
    final TupleEntry args = functionCall.getArguments();
    long nbTriples = 0;
    final EntityDescription eout = (EntityDescription) args.getObject("spo-out");

    eout.setFlowProcess(flowProcess);
    c.objects.clear();
    c.stats.clear();
    final Statements it = eout.iterateStatements();
    while (it.getNext()) {
      final Value object = it.getObject();
      final String predicate = RDFParser.getStringValue(it.getPredicate());

      nbTriples++;
      if (!(object instanceof Literal) && !AnalyticsClassAttributes.isClass(it.getPredicate())) {
        final String o = RDFParser.getStringValue(object);
        if (!c.objects.containsKey(o)) {
          c.objects.put(o, 0L);
        }
        c.objects.put(o, 1 + c.objects.get(o));
      }
      if (!c.stats.containsKey(predicate)) {
        c.stats.put(predicate, 0l);
      }
      c.stats.put(predicate, 1 + (Long) c.stats.get(predicate));
    }

    c.stats.put(NB_TRIPLES, nbTriples);
    final Long[] objects = c.objects.values().toArray(new Long[0]);
    Arrays.sort(objects);
    c.stats.put(OBJECTS, objects);
    try {
      final String json = c.json.writeValueAsString(c.stats);
      c.tuple.set(0, json);
      functionCall.getOutputCollector().add(c.tuple);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
