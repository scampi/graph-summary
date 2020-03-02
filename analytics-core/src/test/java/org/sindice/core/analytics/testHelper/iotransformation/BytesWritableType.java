/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.testHelper.iotransformation;

import org.apache.hadoop.io.BytesWritable;

import cascading.flow.FlowProcess;

/**
 * This {@link FieldType} converts an input string into a {@link BytesWritable}.
 * <p>
 * Each byte is separated by a whitespace, written in base decimal.
 */
public class BytesWritableType
extends AbstractFieldType<BytesWritable> {

  public BytesWritableType(FlowProcess fp, String input) {
    super(fp, input);
  }

  @Override
  protected BytesWritable doConvert() {
    final String[] values = input.split(" ");
    final byte[] bytes = new byte[values.length];

    for (int i = 0; i < values.length; i++) {
      bytes[i] = Byte.valueOf(values[i]);
    }
    return new BytesWritable(bytes);
  }

  @Override
  protected BytesWritable getEmptyField() {
    return new BytesWritable();
  }

}
