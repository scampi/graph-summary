/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.iotransformation;

import org.apache.hadoop.io.BytesWritable;
import org.sindice.core.analytics.testHelper.iotransformation.AbstractFieldType;
import org.sindice.core.analytics.util.Hash;

import cascading.flow.FlowProcess;

/**
 * Convert an input string into its hash value on 128 bits.
 */
public class Hash128Type
extends AbstractFieldType<BytesWritable> {

  public Hash128Type(FlowProcess fp, String s) {
    super(fp, s);
  }

  @Override
  public BytesWritable doConvert() {
    return Hash.getHash128(this.input);
  }

  @Override
  protected BytesWritable getEmptyField() {
    return new BytesWritable();
  }

}
