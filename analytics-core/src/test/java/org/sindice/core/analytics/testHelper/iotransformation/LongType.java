/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.testHelper.iotransformation;


import cascading.flow.FlowProcess;

/**
 * Convert an input {@link String} into an {@link Long}
 */
public class LongType
extends AbstractFieldType<Long> {

  public LongType(FlowProcess fp, String s) {
    super(fp, s);
  }

  @Override
  public Long doConvert() {
    return Long.valueOf(this.input);
  }

  @Override
  protected Long getEmptyField() {
    return 0L;
  }

}
