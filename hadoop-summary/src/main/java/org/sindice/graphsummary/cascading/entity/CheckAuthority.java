/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.entity;

import static org.sindice.core.analytics.util.AnalyticsCounters.ERROR;
import static org.sindice.core.analytics.util.AnalyticsCounters.TIME;
import static org.sindice.graphsummary.cascading.InstanceCounters.AUTH_ENTITIES;
import static org.sindice.graphsummary.cascading.InstanceCounters.INSTANCE_ID;
import static org.sindice.graphsummary.cascading.InstanceCounters.NON_AUTH_ENTITIES;
import static org.sindice.graphsummary.cascading.JobCounters.JOB_ID;

import org.openrdf.model.BNode;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.sindice.analytics.entity.EntityDescription;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
import org.sindice.core.analytics.util.AnalyticsCounters;
import org.sindice.core.analytics.util.Hash;
import org.sindice.core.analytics.util.URIUtil;
import org.sindice.graphsummary.cascading.InstanceCounters;
import org.sindice.graphsummary.cascading.SummaryBaseOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;


/**
 * This {@link Filter} checks the authority of the entity description.
 * 
 * <p>
 * 
 * If the parameter {@link AnalyticsParameters#CHECK_AUTH_TYPE} is <code>true</code>,
 * the authority of an entity is assessed using the elements below:
 * <ul>
 * <li>the 2nd-level domain name of the document(s) where the statements come from; and</li>
 * <li>the 2nd-level domain of the entity URI.</li>
 * </ul>
 * If those two elements are different, the entity is removed.
 * If the entity is a blank node, it is never removed.
 * <p>
 * The counter <b> {@link AnalyticsCounters#ERROR} + CheckAuthority </b> in the {@link InstanceCounters#INSTANCE_ID}
 * group is incremented if the authority of the entity could not be asserted.
 */
public class CheckAuthority
extends SummaryBaseOperation<Void>
implements Filter<Void> {

  private static final long serialVersionUID = -8878500474124408063L;

  private static final Logger logger = LoggerFactory.getLogger(CheckAuthority.class);

  public CheckAuthority() {
    super(3, Fields.ALL);
  }

  @Override
  public boolean isRemove(final FlowProcess flowProcess,
                          final FilterCall<Void> filterCall) {
    if (AnalyticsParameters.CHECK_AUTH_TYPE.get()) {
      final long start = System.currentTimeMillis();

      final TupleEntry args = filterCall.getArguments();

      final Value subject = ((EntityDescription) args.getObject("spo-out")).getEntity();
      if (subject instanceof BNode) {
        flowProcess.increment(JOB_ID, TIME + this.getClass().getSimpleName(), System.currentTimeMillis() - start);
        flowProcess.increment(INSTANCE_ID, AUTH_ENTITIES, 1);
        return false;
      }

      final long pubDomain = args.getLong("domain");
      // Get the hash value of the dataset of the entity.
      final long entityDomain;
      if (subject instanceof URI) {
        final String s = subject.stringValue();
        final String sndDomain = URIUtil.getSndDomainFromUrl(s);
        if (sndDomain == null) {
          logger.error("Unable to extract second-level domain from [{}]", subject);
          flowProcess.increment(INSTANCE_ID, ERROR + CheckAuthority.class.getSimpleName(), 1);
          return true;
        }
        entityDomain = Hash.getHash64(sndDomain);
      } else if (subject instanceof BNode) {
        entityDomain = pubDomain;
      } else {
        throw new IllegalArgumentException("The entity identifier must either be a URI or a blank node.");
      }

      if (entityDomain == pubDomain) {
        flowProcess.increment(JOB_ID, TIME + this.getClass().getSimpleName(), System.currentTimeMillis() - start);
        flowProcess.increment(INSTANCE_ID, AUTH_ENTITIES, 1);
        return false;
      } else {
        flowProcess.increment(JOB_ID, TIME + this.getClass().getSimpleName(), System.currentTimeMillis() - start);
        flowProcess.increment(INSTANCE_ID, NON_AUTH_ENTITIES, 1);
        return true;
      }
    }
    return false;
  }

}
