/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.testHelper.iotransformation;

import cascading.flow.FlowProcess;

/**
 * This {@link FieldType} converts the input string into a <code>byte[]</code>.
 * <p>
 * Each byte is separated by a whitespace, written in base decimal.
 */
public class BytesType
extends AbstractFieldType<byte[]> {

  public BytesType(FlowProcess fp, String input) {
    super(fp, input);
  }

  @Override
  protected byte[] doConvert() {
    final String[] values = input.split(" ");
    final byte[] bytes = new byte[values.length];

    for (int i = 0; i < values.length; i++) {
      bytes[i] = Byte.valueOf(values[i]);
    }
    return bytes;
  }

  @Override
  protected byte[] getEmptyField() {
    return new byte[] {};
  }

}
