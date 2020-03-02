/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.iotransformation;

import org.sindice.core.analytics.testHelper.iotransformation.AbstractFieldType;
import org.sindice.core.analytics.util.Hash;

import cascading.flow.FlowProcess;

/**
 * Convert an input string into its hash value
 */
public class Hash64Type
extends AbstractFieldType<Long> {

  public Hash64Type(FlowProcess fp, String s) {
    super(fp, s);
  }

  @Override
  public Long doConvert() {
    return Hash.getHash64(this.input);
  }

  @Override
  protected Long getEmptyField() {
    return 0L;
  }

}
