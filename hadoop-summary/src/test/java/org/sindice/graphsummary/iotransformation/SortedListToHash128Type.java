/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.iotransformation;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.BytesWritable;
import org.sindice.core.analytics.testHelper.iotransformation.AbstractFieldType;
import org.sindice.core.analytics.util.Hash;

import cascading.flow.FlowProcess;

/**
 * Splits a String on newline <code>\n</code> character, sorts it and returns its {@link Hash hash 128}
 */
public class SortedListToHash128Type
extends AbstractFieldType<BytesWritable> {

  public SortedListToHash128Type(FlowProcess fp, String s) {
    super(fp, s);
  }

  @Override
  public BytesWritable doConvert() {
    final String[] inputSplitted = StringUtils.split(this.input, "\n");
    Arrays.sort(inputSplitted);
    return Hash.getHash128((Object[]) inputSplitted);
  }

  @Override
  protected BytesWritable getEmptyField() {
    return new BytesWritable();
  }

}
