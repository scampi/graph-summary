/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.precision.schema;

import org.sindice.analytics.benchmark.cascading.precision.BlankSwitch;
import org.sindice.analytics.benchmark.cascading.precision.Linksets;
import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsPipe;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.sindice.core.analytics.cascading.operation.CompactionBy;

import cascading.operation.Identity;
import cascading.operation.filter.Not;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.joiner.LeftJoin;
import cascading.tuple.Fields;

/**
 * This {@link SubAssembly} computes the errors related to the schema of the summary, i.e., type and attribute.
 * <p>
 * We assign to the blank node of a summary the union of all type and attribute information associated to the nodes
 * of the gold standard that are contained in the blank node.
 * <p>
 * There is a single head, which comes from the "gold-eval-table" tail of {@link Linksets}.
 * <p>
 * There is a single tail, which fields are the following:
 * <ul>
 * <li><b>domain</b>: the domain of this node;</li>
 * <li><b>cid-eval</b>: the node identifier in the evaluated summary;</li>
 * <li><b>cid-gold</b>: the node identifier in the gold standard summary;</li>
 * <li><b>types-true</b>: the number of true positive type edge;</li>
 * <li><b>types-false</b>: the number of false positive type edge;</li>
 * <li><b>properties-true</b>: the number of true positive attribute edge; and</li>
 * <li><b>properties-false</b>: the number of false positive attribute edge.</li>
 * </ul>
 */
@AnalyticsName(value="schema")
@AnalyticsHeadPipes(values={
  @AnalyticsPipe(sinkName="gold-eval-table", from=Linksets.class)
})
@AnalyticsTailPipes(values={
  @AnalyticsPipe(fields={ "domain", "cid-eval", "cid-gold", "types-true", "types-false",
                          "properties-true", "properties-false" })
})
public class SchemaAssembly
extends AnalyticsSubAssembly {

  private static final long serialVersionUID = -466753824371948803L;

  public SchemaAssembly() {
    super();
  }

  public SchemaAssembly(Pipe[] pipes) {
    super(pipes, (Object[]) null);
  }

  @Override
  protected Object assemble(Pipe[] previous, Object... args) {
    // GL-49 this is to map the current format of types/properties to bytesstream.
    // Update #49: some data from the types / properties BytesStream is discarded, resulting in a custom
    // and smaller BytesStream version. For now this can stay the same.
    // Once the format has been updated to BytesStream in the summarization code, update this too
    previous[0] = new Each(previous[0], new Fields("properties-gold"),
      new PropertiesToTuple(new Fields("properties-gold")), Fields.SWAP);
    previous[0] = new Each(previous[0], new Fields("properties-eval"),
      new PropertiesToTuple(new Fields("properties-eval")), Fields.SWAP);
    previous[0] = new Each(previous[0], new Fields("types-gold"),
      new TypesToTuple(new Fields("types-gold")), Fields.SWAP);
    previous[0] = new Each(previous[0], new Fields("types-eval"),
      new TypesToTuple(new Fields("types-eval")), Fields.SWAP);
    previous[0] = new Each(previous[0], Analytics.getHeadFields(this), new Identity());

    /*
     * Replace the {types,predicates}-eval associated to a cid-eval blank
     */
    Pipe blank = new Pipe("blank", previous[0]);
    blank = new Each(blank, new Fields("cid-eval"), new Not(new BlankFilter()));
    blank = new AggregateBy(blank, new Fields("domain", "cid-eval"),
      new CompactionBy(new Fields("properties-gold"), new Fields("properties-eval")),
      new CompactionBy(new Fields("types-gold"), new Fields("types-eval")));
    final Fields dec = new Fields(
      "domain", "cid-gold", "cid-eval", "c-count-gold", "types-gold", "properties-gold", "t-eval-l", "p-eval-l",
      "d-r", "cid-eval-r", "p-eval-r", "t-eval-r"
    );
    previous[0] = new CoGroup("schema", previous[0], new Fields("domain", "cid-eval"),
      blank, new Fields("domain", "cid-eval"), dec, new LeftJoin());
    previous[0] = new Each(previous[0], new Fields("p-eval-l", "p-eval-r"),
      new BlankSwitch(new Fields("properties-eval")), Fields.SWAP);
    previous[0] = new Each(previous[0], new Fields("t-eval-l", "t-eval-r"),
      new BlankSwitch(new Fields("types-eval")), Fields.SWAP);

    previous[0] = new Each(previous[0], new Fields("properties-gold", "properties-eval"),
      new SchemaFunction(new Fields("properties-true", "properties-false")), Fields.SWAP);
    previous[0] = new Each(previous[0], new Fields("types-gold", "types-eval"),
      new SchemaFunction(new Fields("types-true", "types-false")), Fields.SWAP);

    previous[0] = new Each(previous[0], Analytics.getTailFields(this), new Identity());
    return previous[0];
  }

}
