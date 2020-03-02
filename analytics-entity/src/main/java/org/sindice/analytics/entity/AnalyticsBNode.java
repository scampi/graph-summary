/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.entity;

import java.nio.charset.Charset;

import org.openrdf.model.BNode;

/**
 * This {@link BNode} implementation allows to set the label.
 */
public class AnalyticsBNode
extends AnalyticsValue
implements BNode {

  private static final long serialVersionUID = -8010918015131467391L;

  /**
   * Create an {@link AnalyticsBNode} instance with an empty label.
   */
  public AnalyticsBNode() {
    super();
  }

  /**
   * Create an {@link AnalyticsBNode} instance with the given blank node label (without <b>_:</b>).
   */
  public AnalyticsBNode(String label) {
    this(label.getBytes(Charset.forName("UTF-8")));
  }

  /**
   * Create an {@link AnalyticsBNode} instance with the given blank node label (without <b>_:</b>).
   */
  public AnalyticsBNode(byte[] label) {
    super(label);
  }

  /**
   * Create an {@link AnalyticsBNode} instance with the given blank node label (without <b>_:</b>).
   * The value starts at the position offset, for length bytes.
   */
 public AnalyticsBNode(byte[] value, int offset, int length) {
   super(value, offset, length);
 }

  @Override
  public String getID() {
    return stringValue();
  }

  @Override
  public String toString() {
    return "_:" + stringValue();
  }

  @Override
  public AnalyticsValue getCopy() {
    return new AnalyticsBNode(value.bytes, value.offset, value.length);
  }

  @Override
  public byte getUniqueClassID() {
    return AnalyticsValue.BNODE_CODE;
  }

}
