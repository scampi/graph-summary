/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.util;

import java.io.ByteArrayOutputStream;

/**
 * The {@link ReusableByteArrayOutputStream} extends {@link ByteArrayOutputStream} in order to expose its array of bytes.
 */
public class ReusableByteArrayOutputStream extends ByteArrayOutputStream {

  public ReusableByteArrayOutputStream(int size) {
    super(size);
  }

  /**
   * Returns the backed byte array
   */
  public byte[] getBytes() {
    return buf;
  }

  /**
   * Set the backed byte array
   */
  public void setBytes(byte[] buf) {
    count = buf.length;
    this.buf = buf;
  }

}