/**
 * Copyright 2011, Campinas Stephane
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * @project graph-summary
 * @author Campinas Stephane [ 5 Sep 2011 ]
 * @link stephane.campinas@deri.org
 */
package org.sindice.core.analytics.linkanalysis.ding;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.TupleEntry;

/**
 * 
 */
public class EdgeFilter
extends BaseOperation<Void>
implements Filter<Void> {

  private static final long serialVersionUID = -5667289305504343724L;

  public EdgeFilter() {
    super(7);
  }

  @Override
  public boolean isRemove(FlowProcess flowProcess,
                          FilterCall<Void> call) {
    final TupleEntry args = call.getArguments();

    final String subDomain = args.getString("sub-domain");
    final String objDomain = args.getString("obj-domain");
    final String docDomain = args.getString("doc-domain");

    /*
     * Remove third-party links and intra-relations.
     * Remove also non-authoritative relations.
     */
    if (!subDomain.equals(objDomain) &&
        docDomain.equals(subDomain) && !docDomain.equals(objDomain)) {
      return false;
    }
    return true;
  }

}
