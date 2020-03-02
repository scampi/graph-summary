/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.rdf;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * This {@link ByteArrayInputStream} allows to reset the backed array.
 */
public class ReusableByteArrayInputStream
extends ByteArrayInputStream {

  private static final byte[] EMPTY = {};

  /**
   * Creates a <code>ReusableByteArrayInputStream</code> so that it uses <code>buf</code> as its buffer array.
   * The buffer array is not copied. The initial value of <code>pos</code> is <code>0</code> and the initial value of
   * <code>count</code> is the length of <code>buf</code>.
   * 
   * @param buf
   *          the input buffer.
   */
  public ReusableByteArrayInputStream(byte buf[]) {
    super(buf);
  }

  /**
   * Creates <code>ReusableByteArrayInputStream</code> that uses <code>buf</code> as its buffer array. The initial value
   * of <code>pos</code> is <code>offset</code> and the initial value of <code>count</code> is the minimum of
   * <code>offset+length</code> and <code>buf.length</code>. The buffer array is not copied. The buffer's mark is set to
   * the specified offset.
   * 
   * @param buf
   *          the input buffer.
   * @param offset
   *          the offset in the buffer of the first byte to read.
   * @param length
   *          the maximum number of bytes to read from the buffer.
   */
  public ReusableByteArrayInputStream(byte buf[], int offset, int length) {
    super(buf, offset, length);
  }

  /**
   * Creates a <code>ReusableByteArrayInputStream</code> initialized with an empty byte array.
   */
  public ReusableByteArrayInputStream() {
    this(EMPTY);
  }

  /**
   * Reset this {@link InputStream} to the given bytes array.
   */
  public void reset(final byte[] buf) {
    this.buf = buf;
    this.pos = 0;
    this.count = buf.length;
    this.mark = 0;
  }

}
