/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.entity;

import org.apache.hadoop.io.BytesWritable;
import org.openrdf.model.BNode;
import org.sindice.core.analytics.util.Hash;

/**
 * This class provides means for creating unique {@link BNode}
 * instances across documents.
 */
public class BlankNode {

  private BlankNode() {
  }

  /**
   * Encodes the blank node label so that it is unique across the collection.
   */
  public static BytesWritable encode(final String url, final String bnode) {
    return Hash.getHash128(url, bnode);
  }

  /**
   * Returns the unique hash 128 bits {@link BytesWritable} identifier for the given entity.
   * @param value the {@link AnalyticsValue} to create the hash for
   * @return {@link BytesWritable} the hashed value associated to the blank node
   */
  public static BytesWritable getHash128(final AnalyticsValue value) {
    if (value instanceof AnalyticsBNode) {
      return value.getValueAsBytesWritable();
    }
    return Hash.getHash128(value.getValue());
  }

}
