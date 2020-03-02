/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */

package org.sindice.analytics.entity;

import static org.sindice.core.analytics.util.AnalyticsCounters.GROUP;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;

/**
 * Base class for an {@link EntityDescription} implementation.
 */
public abstract class AbstractEntityDescription
implements EntityDescription {

  private static final Logger logger = LoggerFactory.getLogger(AbstractEntityDescription.class);

  protected FlowProcess fp = FlowProcess.NULL;

  /**
   * A list of ranges in decreased order, used for logging large entities.
   */
  private final static long[] RANGES = { 100000, 50000 };

  /**
   * @param fp the {@link FlowProcess} to set
   */
  @Override
  public void setFlowProcess(FlowProcess fp) {
    this.fp = fp;
  }

  /**
   * Logs large entities, based on the {@link #RANGES}.
   * Increments the Hadoop counter <b>{upper} &gt; LARGE_ENTITIES &gt;= {lower}</b> if
   * the number of triples lies in the range {@code [lower, upper[}.
   * @param nbTriples the number of triples about this entity
   */
  protected void logLargeEntity(long nbTriples) {
    long upper = RANGES[0];

    for (long range : RANGES) {
      if (nbTriples >= range) {
        logger.warn("Found entity with more than " + range + " (exactly {}) triples: [{}]", nbTriples, getEntity());
        if (upper != range) {
          fp.increment(GROUP, upper + " > LARGE_ENTITIES >= " + range, 1);
        } else {
          fp.increment(GROUP, "LARGE_ENTITIES >= " + range, 1);
        }
        break;
      }
      upper = range;
    }
  }

}
