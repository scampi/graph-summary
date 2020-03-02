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

import cascading.operation.Identity;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;

public class DingFlow
extends SubAssembly {

  private static final long serialVersionUID = 4413071377651676399L;
  public static final Fields DECLARATION_FIELDS = new Fields("s-index", "vect-weights");

  public DingFlow(Pipe...previous) {
    setTails(assemble(previous));
  }

  private Pipe assemble(Pipe...previous) {
    // Remove unwanted edges
    Pipe pipe = new Each(previous[0], DingProcess.SUMMARY_FIELDS, new EdgeFilter());
    final Fields linksets = new Fields("sub-domain", "predicate-hash", "obj-domain", "total");
    pipe = new Each(pipe, linksets, new Identity());
    // Aggregate inter-datasets relations
    pipe = new SumBy(pipe, new Fields("sub-domain", "obj-domain", "predicate-hash"), new Fields("total"), new Fields("total"), Long.class);

    // Map domain to their index value
    pipe = new CoGroup(pipe, new Fields("sub-domain"), previous[1], new Fields("dataset"));
    pipe = new Each(pipe, new Fields("index", "obj-domain", "predicate-hash", "total"), new Identity(new Fields("s-index", "obj-domain", "predicate-hash", "total")));
    pipe = new CoGroup(pipe, new Fields("obj-domain"), previous[1], new Fields("dataset"));
    pipe = new Each(pipe, new Fields("s-index", "index", "predicate-hash", "total"), new Identity(new Fields("s-index", "o-index", "predicate", "total")));
    // Add the Predicate Distribution to the tuple
    pipe = new CoGroup(pipe, new Fields("predicate"), previous[2], new Fields("predicate-hash"));
    pipe = new Each(pipe, new Fields("s-index", "o-index", "datasets", "total"), new Identity(new Fields("s-index", "o-index", "p-dist", "total")));

    // Compute LF-IDF weights matrix
    pipe = new GroupBy(pipe, new Fields("s-index"));
    pipe = new Every(pipe, new Fields("s-index", "o-index", "p-dist", "total"), new LfIdfAggregator(DECLARATION_FIELDS), Fields.RESULTS);
    return pipe;
  }

}
