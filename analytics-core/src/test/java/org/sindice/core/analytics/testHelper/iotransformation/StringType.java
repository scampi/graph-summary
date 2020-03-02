/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.testHelper.iotransformation;

import cascading.flow.FlowProcess;

/**
 * Takes a string and return the same
 *
 */
public class StringType
extends AbstractFieldType<String> {

  public StringType(FlowProcess fp, String s) {
    super(fp, s);
  }

  @Override
  protected String doConvert() {
    return this.input;
  }

  @Override
  protected String getEmptyField() {
    return "";
  }

}
