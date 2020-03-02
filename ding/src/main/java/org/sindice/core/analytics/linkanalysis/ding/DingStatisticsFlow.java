/**
 * Copyright (c) 2009-2011 Sindice Limited. All Rights Reserved. Project and
 * contact information: http://www.siren.sindice.com/ This file is part of the
 * SIREn project. SIREn is a free software: you can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version. SIREn is distributed in the hope that
 * it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Affero General Public License for more details. You should have received a
 * copy of the GNU Affero General Public License along with SIREn. If not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.sindice.core.analytics.linkanalysis.ding;

import java.util.HashMap;

import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.operation.state.Counter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.Unique;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class DingStatisticsFlow
extends SubAssembly {

  private static final long  serialVersionUID = -1135522536387226519L;
  public static final Fields D2I_FIELDS       = new Fields("dataset", "index");
  public static final Fields PDIST_FIELDS     = new Fields("predicate-hash", "datasets");
  public static final Fields D_NODE_CARD      = new Fields("dataset", "nodes-cardinality");

  public DingStatisticsFlow(Pipe...previous) {
    setTails(assemble(previous));
  }

  private Pipe[] assemble(Pipe...previous) {
    // Remove unwanted edges
    Pipe pipe = new Each(previous[0], DingProcess.SUMMARY_FIELDS, new EdgeFilter());

    // Dataset index + Number of datasets
    Pipe dsIndex = new Pipe("Dataset2Index", pipe);
    dsIndex = new Each(dsIndex, new Fields("sub-domain", "obj-domain"), new FieldSpliter(new Fields("dataset")), Fields.RESULTS);
    dsIndex = new Unique(dsIndex, new Fields("dataset"));
    dsIndex = new Each(dsIndex, new Fields("dataset"), new Insert(new Fields("group"), 0), Fields.ALL);
    dsIndex = new GroupBy(dsIndex, new Fields("group"), new Fields("dataset"));
    dsIndex = new Every(dsIndex, new Fields("dataset"), new Index(new Fields("dataset", "index")), Fields.ALL);
    dsIndex = new Each(dsIndex, new Counter("Datasets", "Cardinality"));

    // Predicate Distribution
    Pipe pDist = new Pipe("Predicate-Dist", pipe);
    pDist = new Each(pDist, new Fields("predicate-hash", "sub-domain"), new Identity());
    pDist = new Unique(pDist, new Fields("predicate-hash", "sub-domain"));
    pDist = new CountBy(pDist, new Fields("predicate-hash"), new Fields("datasets"));
    pDist = new Each(pDist, new Insert(new Fields("group"), 0), Fields.ALL);
    pDist = new GroupBy(pDist, new Fields("group"), new Fields("predicate-hash"));

    /*
     * Dataset Cardinality: Number of node collection in a dataset
     * WARNING: this number only takes into account nodes that are left after the filtering!
     */
    Pipe dNodesCardSub = new Each(pipe, new Fields("sub-types", "sub-domain"), new Identity(new Fields("types", "tmp-dataset")));
    Pipe dNodesCardObj = new Each(pipe, new Fields("obj-types", "obj-domain"), new Identity(new Fields("types", "tmp-dataset")));
    dNodesCardObj = new GroupBy(Pipe.pipes(dNodesCardObj, dNodesCardSub));
    dNodesCardObj = new Unique(dNodesCardObj, new Fields("types", "tmp-dataset"));
    dNodesCardObj = new CountBy(dNodesCardObj, new Fields("tmp-dataset"), new Fields("nodes-cardinality"));
    // Join with the Dataset2Index values
    dNodesCardObj = new CoGroup(dNodesCardObj, new Fields("tmp-dataset"), dsIndex, new Fields("dataset"));
    dNodesCardObj = new Each(dNodesCardObj, new Fields("index", "nodes-cardinality"), new Identity(new Fields("dataset", "nodes-cardinality")));
    Pipe dNodesCard = new Pipe("Dataset-Node-Cardinality", dNodesCardObj);

    return Pipe.pipes(dsIndex, pDist, dNodesCard);
  }

  public HashMap<String, Tap> getSinks(String outputPath) {
    final HashMap<String, Tap> sinks = new HashMap<String, Tap>();

    sinks.put("Dataset2Index", new Hfs(new SequenceFile(D2I_FIELDS), outputPath + "/Dataset2Index"));
    sinks.put("Predicate-Dist", new Hfs(new SequenceFile(PDIST_FIELDS), outputPath + "/predicate-dist"));
    sinks.put("Dataset-Node-Cardinality", new Hfs(new SequenceFile(D_NODE_CARD), outputPath + "/datasets-nodes"));
    return sinks;
  }

}
