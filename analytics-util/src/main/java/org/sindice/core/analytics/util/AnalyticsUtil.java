/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.util;

import java.io.DataOutput;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableUtils;

import cascading.flow.FlowProcess;

/**
 * The {@link AnalyticsUtil} class provides a set of utility methods for Cascading.
 */
public class AnalyticsUtil {

  private AnalyticsUtil() {}

  /**
   * Returns <code>true</code> if local execution.
   */
  public static boolean isHadoopLocalMode(Configuration conf) {
    return "local".equals(conf.get("mapred.job.tracker"));
  }

  /**
   * Compare the two maps according to a lexicographic order.
   * The keys of the maps should be sort prior to this call.
   * @param a of type {@link Map} with {@link Comparable} key and value
   * @param b of type {@link Map} with {@link Comparable} key and value
   * @return a negative integer, zero, or a positive integer as <b>a</b> is less
   * than, equal to, or greater than <b>b</b>.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static int compareMaps(Map<? extends Comparable, ? extends Comparable> a,
                                Map<? extends Comparable, ? extends Comparable> b) {
    final Iterator thisSet = a.entrySet().iterator();
    final Iterator otherSet = b.entrySet().iterator();

    while (thisSet.hasNext() && otherSet.hasNext()) {
      final Entry<Comparable, Comparable> thisPair = (Entry<Comparable, Comparable>) thisSet.next();
      final Entry<Comparable, Comparable> otherPair = (Entry<Comparable, Comparable>) otherSet.next();
      if (thisPair.getKey().equals(otherPair.getKey())) {
        final int c = thisPair.getValue().compareTo(otherPair.getValue());
        if (c != 0) {
          return c;
        }
      } else {
        return thisPair.getKey().compareTo(otherPair.getKey());
      }
    }
    return a.size() - b.size();
  }

  /**
   * Convert a long value to a byte array using big-endian.
   * <p>
   * Copied from HBase Bytes#toBytes(long) in order to pass the array b.
   *
   * @param val value to convert
   * @param b the byte array to put in the long value
   * @param offset the offset in b to start writing the data
   * @throws IndexOutOfBoundsException if there is not at least 8 bytes available starting at the offset position
   */
  public static void writeLong(long val, byte[] b, int offset) {
    if (offset + 8 > b.length) {
      throw new IndexOutOfBoundsException("Not enough space in b at the offset [" + offset +
        "] for writing the long value.");
    }
    for (int i = 7; i > 0; i--) {
      b[i + offset] = (byte) val;
      val >>>= 8;
    }
    b[offset] = (byte) val;
  }

  /**
   * Convert an int value to a byte array.
   * <p>
   * Copied from HBase Bytes#toBytes(int) in order to pass the array b.
   * @param val value to convert
   * @param b the byte array to put in the long value
   * @param offset the offset in b to start writing the data
   * @throws IndexOutOfBoundsException if there is not at least 4 bytes available starting at the offset position
   */
  public static byte[] writeInt(int val, byte[] b, int offset) {
    if (offset + 4 > b.length) {
      throw new IndexOutOfBoundsException("Not enough space in b at the offset [" + offset +
        "] for writing the int value.");
    }

    for(int i = 3; i > 0; i--) {
      b[i + offset] = (byte) val;
      val >>>= 8;
    }
    b[offset] = (byte) val;
    return b;
  }

  /**
   * Serializes a long to a binary stream with zero-compressed encoding.
   * For -112 <= i <= 127, only one byte is used with the actual value.
   * For other values of i, the first byte value indicates whether the
   * long is positive or negative, and the number of bytes that follow.
   * If the first byte value v is between -113 and -120, the following long
   * is positive, with number of bytes that follow are -(v+112).
   * If the first byte value v is between -121 and -128, the following long
   * is negative, with number of bytes that follow are -(v+120). Bytes are
   * stored in the high-non-zero-byte-first order.
   * <p>
   * Copied from {@link WritableUtils#writeVLong(DataOutput, long)} to operate on byte[] instead of on stream.
   * @param bytes the byte[] array to write the VByte-encoded long into
   * @param offset the start in the array
   * @param i Long to be serialized
   * @throws ArrayIndexOutOfBoundsException if the array is too small to contain the encoded long
   */
  public static void writeVLong(byte[] bytes, int offset, long i) {
    if (i >= -112 && i <= 127) {
      bytes[offset] = (byte) i;
      return;
    }

    int len = -112;
    if (i < 0) {
      i ^= -1L; // take one's complement'
      len = -120;
    }

    long tmp = i;
    while (tmp != 0) {
      tmp = tmp >> 8;
      len--;
    }

    bytes[offset++] = (byte) len;

    len = (len < -120) ? -(len + 120) : -(len + 112);

    for (int idx = len; idx != 0; idx--) {
      final int shiftbits = (idx - 1) * 8;
      final long mask = 0xFFL << shiftbits;
      bytes[offset++] = (byte) ((i & mask) >> shiftbits);
    }
  }

  /**
   * Reads a zero-compressed encoded int from the byte array and returns it.
   * <p>
   * Copied from {@link WritableUtils#readVInt(java.io.DataInput)}.
   * @param bytes the bytes to decode
   * @param off the offset in the array
   * @return deserialized int from {@code byte[]}.
   */
  public static int readVInt(byte[] bytes, int off) {
    return (int) readVLong(bytes, off);
  }

  /**
   * Reads a zero-compressed encoded long from the byte array and returns it.
   * <p>
   * Copied from {@link WritableUtils#readVLong(java.io.DataInput)}.
   * @param bytes the bytes to decode
   * @param off the offset in the array
   * @return deserialized long from {@code byte[]}.
   */
  public static long readVLong(byte[] bytes, int off) {
    final byte firstByte = bytes[off++];
    final int len = WritableUtils.decodeVIntSize(firstByte);

    if (len == 1) {
      return firstByte;
    }

    long i = 0;
    for (int idx = 0; idx < len-1; idx++) {
      final byte b = bytes[off++];
      i = i << 8;
      i = i | (b & 0xFF);
    }
    return (WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1L) : i);
  }

  /**
   * Increment a counter based on the range the value falls in.
   * <p>
   * Given the value <code>9</code> and the ranges <code>{ 5, 10 }</code>, this method will increment a counter as
   * follows:
   * 
   * <pre>
   * fp.increment(groupId, counterId + &quot; &lt;= 10&quot;, 1);
   * </pre>
   * 
   * @param fp
   *          of type {@link FlowProcess}
   * @param groupId
   *          the group identifier
   * @param counterId
   *          the counter identifier
   * @param value
   *          the value to compare against the set of ranges
   * @param ranges
   *          the {@code long[]} array of ranges
   */
  public static void incrementByRange(final FlowProcess fp,
                                      final String groupId,
                                      final String counterId,
                                      final long value,
                                      final long... ranges) {
    if (ranges == null || ranges.length == 0) {
      throw new IllegalArgumentException("You must provide an array of ranges");
    }
    Arrays.sort(ranges);
    long prev = 0;
    for (long range : ranges) {
      if (value <= range) {
        fp.increment(groupId, prev + " < " + counterId + " <= " + range, 1);
        return;
      }
      prev = range;
    }
    fp.increment(groupId, counterId + " > " + ranges[ranges.length - 1], 1);
  }

}
