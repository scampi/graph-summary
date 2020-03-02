/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.iotransformation;

import org.sindice.core.analytics.testHelper.iotransformation.AbstractFieldType;

import cascading.flow.FlowProcess;

/**
 * Convert an input {@link String} into an {@link Double}
 */
public class DoubleType
extends AbstractFieldType<Double> {

  public DoubleType(FlowProcess fp, String s) {
    super(fp, s);
  }

  @Override
  public Double doConvert() {
    return Double.valueOf(this.input);
  }

  @Override
  protected Double getEmptyField() {
    return 0D;
  }

}
