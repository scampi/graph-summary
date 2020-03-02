/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.hash2value;

import static org.sindice.core.analytics.util.AnalyticsCounters.TUPLES_READ;
import static org.sindice.core.analytics.util.AnalyticsCounters.TUPLES_WRITTEN;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import org.apache.hadoop.filecache.DistributedCache;
import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.sindice.core.analytics.cascading.operation.DifferentValueFilter;

import cascading.operation.state.Counter;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Unique;
import cascading.tuple.Fields;

/**
 * This {@link AnalyticsSubAssembly} generates dictionaries between hashes and values
 * for predicates, classes, domains, and datatypes of the data graph.
 */
@AnalyticsName(value="dictionary")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(fields={ "value" })
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(name="type", fields={ "hash", "string" }),
  @AnalyticsPipe(name="predicate", fields={ "hash", "string" }),
  @AnalyticsPipe(name="domain", fields={ "hash", "string" }),
  @AnalyticsPipe(name="datatype", fields={ "hash", "string" })
})
public class GenerateDictionary extends AnalyticsSubAssembly {

  private static final long  serialVersionUID = -5900930328532366854L;

  /** Label for the domain dictionary in the {@link DistributedCache} */
  public static final String DOMAIN_PREFIX    = "domain-map";

  /** Label for the type dictionary in the {@link DistributedCache} */
  public static final String CLASS_PREFIX     = "class-map";

  /** Label for the predicate dictionary in the {@link DistributedCache} */
  public static final String PREDICATE_PREFIX = "predicate-map";

  /** Label for the datatype dictionary in the {@link DistributedCache} */
  public static final String DATATYPE_PREFIX  = "datatype-map";

  public GenerateDictionary() {
    super();
  }

  public GenerateDictionary(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    Pipe pipe = new Each(previous[0], new Counter(JOB_ID, TUPLES_READ + name));

    final Fields declarationFields = new Fields("label", "hash", "string");

    // Hash triples and output a tuple for the domain, type, and predicate
    pipe = new Each(pipe, new GenerateDictionaryFunction(declarationFields));

    // Remove duplicate results, unwanted in dictionary
    pipe = new Unique(pipe, new Fields("label", "hash"));
    pipe = new GroupBy(pipe, new Fields("label"), new Fields("hash"));

    /*
     * Type
     */
    Pipe pipeT = new Pipe("type", pipe);
    pipeT = new Each(pipeT, new Fields("label"), new DifferentValueFilter(GenerateDictionaryFunction.TYPE_FLAG));
    pipeT = new Each(pipeT, new Counter(JOB_ID, TUPLES_WRITTEN + name));
    /*
     * Domain
     */
    Pipe pipeD = new Pipe("domain", pipe);
    pipeD = new Each(pipeD, new Fields("label"), new DifferentValueFilter(GenerateDictionaryFunction.DOMAIN_FLAG));
    pipeD = new Each(pipeD, new Counter(JOB_ID, TUPLES_WRITTEN + name));
    /*
     * Predicate
     */
    Pipe pipeP = new Pipe("predicate", pipe);
    pipeP = new Each(pipeP, new Fields("label"), new DifferentValueFilter(GenerateDictionaryFunction.PREDICATE_FLAG));
    pipeP = new Each(pipeP, new Counter(JOB_ID, TUPLES_WRITTEN + name));
    /*
     * Datatype
     */
    Pipe pipeDT = new Pipe("datatype", pipe);
    pipeDT = new Each(pipeDT, new Fields("label"), new DifferentValueFilter(GenerateDictionaryFunction.DATATYPE_FLAG));
    pipeDT = new Each(pipeDT, new Counter(JOB_ID, TUPLES_WRITTEN + name));

    return Pipe.pipes(pipeT, pipeP, pipeD, pipeDT);
  }

}
