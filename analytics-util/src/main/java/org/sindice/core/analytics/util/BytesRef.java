package org.sindice.core.analytics.util;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.nio.charset.Charset;

import org.apache.hadoop.io.WritableComparator;

/** Represents byte[], as a slice (offset + length) into an
 *  existing byte[]. The {@link #bytes} member should never be null;
 *  use {@link #EMPTY_BYTES} if necessary.
 *  <p>
 *  Copied from Lucene BytesRef for the Analytics project use case.
 */
public final class BytesRef implements Comparable<BytesRef>, Cloneable {

  /** An empty byte array for convenience */
  public static final byte[] EMPTY_BYTES = new byte[0]; 

  /** The contents of the BytesRef. Should never be {@code null}. */
  public byte[] bytes;

  /** Offset of first valid byte. */
  public int offset;

  /** Length of used bytes. */
  public int length;

  /** Create a BytesRef with {@link #EMPTY_BYTES} */
  public BytesRef() {
    this(EMPTY_BYTES);
  }

  /**
   * This instance will directly reference bytes w/o making a copy.
   * bytes should not be <code>null</code>.
   */
  public BytesRef(byte[] bytes, int offset, int length) {
    assert bytes != null;
    assert offset >= 0;
    assert length >= 0;
    assert bytes.length >= offset + length;
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
  }

  /**
   * This instance will directly reference bytes w/o making a copy.
   * bytes should not be <code>null</code>
   */
  public BytesRef(byte[] bytes) {
    this(bytes, 0, bytes.length);
  }

  /** 
   * Create a BytesRef pointing to a new array of size <code>capacity</code>.
   * Offset and length will both be zero.
   */
  public BytesRef(int capacity) {
    this.bytes = new byte[capacity];
  }

  /**
   * Returns this {@link BytesRef} as a String using the <tt>UTF-8</tt> encoding.
   */
  public String asUTF8() {
    return new String(bytes, offset, length, Charset.forName("UTF-8"));
  }

  @Override
  public BytesRef clone() {
    return new BytesRef(bytes, offset, length);
  }

  /**
   * Calculates the hash code using {@link WritableComparator#hashBytes(byte[], int, int)}.
   */
  @Override
  public int hashCode() {
    return WritableComparator.hashBytes(bytes, offset, length);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other instanceof BytesRef) {
      return compareTo((BytesRef) other) == 0;
    }
    return false;
  }

  /** Returns hex encoded bytes, eg [0x6c 0x75 0x63 0x65 0x6e 0x65] */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    final int end = offset + length;
    for(int i=offset;i<end;i++) {
      if (i > offset) {
        sb.append(' ');
      }
      sb.append(Integer.toHexString(bytes[i]&0xff));
    }
    sb.append(']');
    return sb.toString();
  }

  /**
   * Copies the bytes from the given {@link BytesRef}
   * <p>
   * NOTE: if this would exceed the array size, this method creates a 
   * new reference array.
   */
  public void copyBytes(BytesRef other) {
    if (bytes.length - offset < other.length) {
      bytes = new byte[other.length];
      offset = 0;
    }
    System.arraycopy(other.bytes, other.offset, bytes, offset, other.length);
    length = other.length;
  }

  /**
   * Appends the bytes from the given {@link BytesRef}
   * <p>
   * NOTE: if this would exceed the array size, this method creates a 
   * new reference array.
   */
  public void append(BytesRef other) {
    int newLen = length + other.length;
    if (bytes.length - offset < newLen) {
      byte[] newBytes = new byte[newLen];
      System.arraycopy(bytes, offset, newBytes, 0, length);
      offset = 0;
      bytes = newBytes;
    }
    System.arraycopy(other.bytes, other.offset, bytes, length+offset, other.length);
    length = newLen;
  }

  /** Unsigned byte order comparison */
  public int compareTo(BytesRef other) {
    return WritableComparator.compareBytes(bytes, offset, length,
      other.bytes, other.offset, other.length);
  }

  /**
   * Creates a new BytesRef that points to a copy of the bytes from 
   * <code>other</code>
   * <p>
   * The returned BytesRef will have a length of other.length
   * and an offset of zero.
   */
  public static BytesRef deepCopyOf(BytesRef other) {
    BytesRef copy = new BytesRef();
    copy.copyBytes(other);
    return copy;
  }

}
