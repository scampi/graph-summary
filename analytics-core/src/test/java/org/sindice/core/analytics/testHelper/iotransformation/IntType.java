/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.testHelper.iotransformation;

import cascading.flow.FlowProcess;

/**
 * This {@link FieldType} converts the input into a {@link Integer}.
 */
public class IntType
extends AbstractFieldType<Integer> {

  public IntType(FlowProcess fp, String s) {
    super(fp, s);
  }

  @Override
  protected Integer doConvert() {
    return Integer.valueOf(this.input);
  }

  @Override
  protected Integer getEmptyField() {
    return 0;
  }

}
