/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.util;

import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;

/**
 * Set of methods for hashing objects using {@link MurmurHash3}.
 */
public class Hash {

  private static final StringBuffer sb = new StringBuffer();

  private Hash() {}

  /**
   * Compute the hash with 32 bits
   */
  public static int getHash32(Object s) {
    return MurmurHash3.hash(s);
  }

  /**
   * Compute the hash with 32 bits
   */
  public static int getHash32(Object s1, Object s2) {
    sb.setLength(0);
    sb.append(s1).append(s2);
    return MurmurHash3.hash(sb.toString());
  }

  /**
   * Compute the hash with 64 bits
   */
  public static long getHash64(Object s) {
    return MurmurHash3.hashLong(s);
  }

  /**
   * Compute the hash with 64 bits
   */
  public static long getHash64(BytesRef bref) {
    return getHash64(bref.bytes, bref.offset, bref.length);
  }

  /**
   * Compute the hash with 64 bits
   */
  public static long getHash64(String s) {
    return MurmurHash3.hashLong(s);
  }

  /**
   * Compute the hash with 64 bits
   */
  public static long getHash64(byte[] s) {
    return MurmurHash3.hashLong(s);
  }

  /**
   * Compute the hash with 64 bits
   */
  public static long getHash64(Object s1, Object s2) {
    sb.setLength(0);
    sb.append(s1).append(s2);
    return MurmurHash3.hashLong(sb.toString());
  }

  /**
   * Compute the hash with 64 bits
   */
  public static long getHash64(Object...objects) {
    sb.setLength(0);
    for (Object o : objects) {
      sb.append(o);
    }
    return MurmurHash3.hashLong(sb.toString());
  }

  /**
   * Compute the hash with 64 bits
   */
  public static long getHash64(byte[] bytes, int off, int len) {
    return MurmurHash3.MurmurHash3_x64_128(bytes, off, len, 9001)[0];
  }

  /**
   * Compute the hash with 64 bits
   */
  public static long getHash64(List<Object> toHash) {
    sb.setLength(0);
    for (Object s : toHash) {
      sb.append(s);
    }
    return getHash64(sb.toString());
  }

  /**
   * Compute the hash with 128 bits
   */
  public static BytesWritable getHash128(Iterable<?> it) {
    final Iterator<?> ite = it.iterator();

    sb.setLength(0);
    while (ite.hasNext()) {
      sb.append(ite.next());
    }
    return getHash128(sb.toString().getBytes(Charset.forName("UTF-8")));
  }

  /**
   * Compute the hash with 128 bits
   */
  public static BytesWritable getHash128(Object... toHash) {
    sb.setLength(0);

    for (Object o : toHash) {
      sb.append(o);
    }
    return getHash128(sb.toString().getBytes(Charset.forName("UTF-8")));
  }

  /**
   * Compute the hash with 128 bits
   */
  public static BytesWritable getHash128(byte[] bytes) {
    return getHash128(bytes, 0, bytes.length);
  }

  /**
   * Compute the hash with 128 bits
   */
  public static BytesWritable getHash128(BytesRef bytes) {
    return getHash128(bytes.bytes, bytes.offset, bytes.length);
  }

  /**
   * Compute the hash with 128 bits
   */
  public static BytesWritable getHash128(byte[] bytes, int off, int len) {
    final long[] hash = MurmurHash3.MurmurHash3_x64_128(bytes, off, len, 9001);

    final byte[] value = new byte[16];
    AnalyticsUtil.writeLong(hash[0], value, 0);
    AnalyticsUtil.writeLong(hash[1], value, 8);
    return new BytesWritable(value);
  }

}
