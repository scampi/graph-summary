/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.iotransformation;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.BytesWritable;
import org.sindice.core.analytics.testHelper.iotransformation.AbstractFieldType;
import org.sindice.core.analytics.testHelper.iotransformation.FieldType;
import org.sindice.core.analytics.util.AnalyticsUtil;
import org.sindice.core.analytics.util.Hash;

import cascading.flow.FlowProcess;

/**
 * This {@link FieldType} converts the input string into a {@link BytesWritable}, where each line is
 * {@link Hash#getHash64(String) hashed}.
 */
public class Hash64ListType
extends AbstractFieldType<BytesWritable> {

  public Hash64ListType(FlowProcess fp, String s) {
    super(fp, s);
  }

  @Override
  public BytesWritable doConvert() {
    final String[] inputSplitted = StringUtils.split(this.input, "\n");
    final byte[] bytes = new byte[inputSplitted.length * 8];

    for (int i = 0; i < inputSplitted.length; i++) {
      AnalyticsUtil.writeLong(Hash.getHash64(inputSplitted[i]), bytes, i * 8);
    }
    return new BytesWritable(bytes);
  }

  @Override
  protected BytesWritable getEmptyField() {
    return new BytesWritable();
  }

}
