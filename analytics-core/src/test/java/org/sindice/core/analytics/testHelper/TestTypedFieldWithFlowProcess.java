/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.testHelper;

import org.sindice.core.analytics.testHelper.iotransformation.AbstractFieldType;

import cascading.flow.FlowProcess;

/**
 * This class is used in {@link TestCheckScheme} for testing purpose.
 * This class appends to a value defined in the {@link FlowProcess} via the parameter {@value #DATA}
 * the content of the field.
 */
public class TestTypedFieldWithFlowProcess extends AbstractFieldType<String> {

  public final static String DATA = "data";

  public TestTypedFieldWithFlowProcess(FlowProcess fp, String input) {
    super(fp, input);
  }

  @Override
  public String doConvert() {
    return fp.getStringProperty(DATA) + input;
  }

  @Override
  protected String getEmptyField() {
    return fp.getStringProperty(DATA);
  }

}