/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.scheme;

import java.util.Arrays;

import org.apache.hadoop.hbase.KeyValue.KeyComparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.sindice.core.analytics.util.AnalyticsException;
import org.sindice.core.analytics.util.AnalyticsUtil;

import cascading.tuple.hadoop.io.HadoopTupleInputStream;
import cascading.tuple.hadoop.io.HadoopTupleOutputStream;
import cascading.tuple.io.TupleOutputStream;

/**
 * This {@link KeyComparator} is used for comparing keys that were serialized using {@link TupleOutputStream}
 * in {@link HFileScheme}.
 * <p>
 * The key is a byte[] which contains a VByte encoded flag indicating the class, with the relevant data after.
 * @see HadoopTupleInputStream
 * @see HadoopTupleOutputStream
 */
public class AnalyticsKeyComparator
extends KeyComparator {

  @Override
  public int compare(byte[] left, byte[] right) {
    return compare(left, 0, left.length, right, 0, right.length);
  }

  @Override
  public int compare(byte[] left, int loffset, int llength, byte[] right, int roffset, int rlength) {
    final int lHeaderSize = WritableUtils.decodeVIntSize(left[loffset]);
    final int rHeaderSize = WritableUtils.decodeVIntSize(right[roffset]);

    if (lHeaderSize != rHeaderSize) {
      throw new AnalyticsException("Headers of different size: left=" + Arrays.toString(left) + " lOff=" + loffset +
        " lLen=" + llength + " right=" + Arrays.toString(right) + " rOff=" + roffset + " rLen=" + rlength);
    }
    final int lHeader = AnalyticsUtil.readVInt(left, loffset);
    final int rHeader = AnalyticsUtil.readVInt(right, roffset);

    if (lHeader != rHeader) {
      throw new AnalyticsException("Receieved keys of different type: lHeader=" + lHeader + " rHeader=" + rHeader);
    }
    switch (lHeader) {
      case 5: // VLong
        final long lVal = AnalyticsUtil.readVLong(left, loffset + lHeaderSize);
        final long rVal = AnalyticsUtil.readVLong(right, roffset + rHeaderSize);
        return (lVal < rVal ? -1 : (lVal == rVal ? 0 : 1));
      case 1: // String
      case 126: // byte[] (see BytesSerialization)
      case 127: // BytesWritable (see TupleSerialization)
        // the size of the data is stored as a int
        return WritableComparator.compareBytes(left, loffset + lHeaderSize + 4, llength - lHeaderSize - 4, right,
          roffset + rHeaderSize + 4, rlength - rHeaderSize - 4);
      default:
        throw new AnalyticsException("Not supported key type: " + lHeader);
    }
  }

}
