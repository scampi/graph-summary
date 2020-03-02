/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.dictionary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.AnalyticsCacheConfig;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.sindice.core.analytics.cascading.scheme.HFileScheme;
import org.sindice.core.analytics.util.AnalyticsCounters;
import org.sindice.core.analytics.util.AnalyticsException;
import org.sindice.core.analytics.util.AnalyticsUtil;
import org.sindice.core.analytics.util.ReusableByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tuple.Tuple;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.hadoop.TupleSerialization.SerializationElementReader;
import cascading.tuple.hadoop.TupleSerialization.SerializationElementWriter;
import cascading.tuple.hadoop.io.BufferedInputStream;
import cascading.tuple.hadoop.io.HadoopTupleInputStream;
import cascading.tuple.hadoop.io.HadoopTupleOutputStream;
import cascading.tuple.io.TupleInputStream;
import cascading.tuple.io.TupleOutputStream;

/**
 * A {@link HFile}-based implementation of the {@link Dictionary}.
 * <p>
 * It relies on the {@link TupleOutputStream} and {@link TupleInputStream} for converting the key to bytes, and the
 * retrieved value back to the object.
 * <p>
 * The counter <b>Error_HFileDictionary</b> in the group {@link AnalyticsCounters#GROUP} counts the number of keys
 * that were not found.
 */
public class HFileDictionary implements Dictionary {

  protected final static Logger       logger = LoggerFactory.getLogger(HFileDictionary.class);

  private HFileScanner[]              scanners;

  private final FlowProcess           fp;

  final TupleInputStream              in;
  final TupleOutputStream             out;
  final BufferedInputStream           bytesIn;
  final ReusableByteArrayOutputStream bytesOut;

  HFileDictionary(FlowProcess flowProcess) {
    final TupleSerialization ser = new TupleSerialization(flowProcess);
    final SerializationElementReader r = new SerializationElementReader(ser);
    final SerializationElementWriter w = new SerializationElementWriter(ser);

    this.fp = flowProcess;
    bytesIn = new BufferedInputStream();
    bytesOut = new ReusableByteArrayOutputStream(1024);
    in = new HadoopTupleInputStream(bytesIn, r);
    out = new HadoopTupleOutputStream(bytesOut, w);
  }

  @Override
  public Object getValue(Object key) {
    if (scanners == null) {
      return null;
    }

    try {
      for (HFileScanner scan : scanners) {
        if (scan == null) {
          continue;
        }

        // Convert the key to byte
        bytesOut.reset();
        if (key instanceof Tuple) {
          out.writeTuple((Tuple) key);
        } else {
          out.writeElement(key);
        }
        // Search for the byte[] key
        final byte[] value = doGetValue(scan, bytesOut.getBytes(), 0, bytesOut.size());
        if (value != null) {
          bytesIn.reset(value, 1, value.length); // skip the header
          if (value[0] == HFileScheme.IS_TUPLE) {
            return in.readTuple();
          } else if (value[0] == HFileScheme.IS_NOT_TUPLE) {
            return in.getNextElement();
          } else {
            // should not happen
            throw new AnalyticsException();
          }
        }
      }
    } catch (IOException e) {
      logger.error("No Value found for key=[" + key + "]", e);
    }
    fp.increment(AnalyticsCounters.GROUP, AnalyticsCounters.ERROR + HFileDictionary.class.getSimpleName(), 1);
    return null;
  }

  /**
   * Searches for the key using the given {@link HFileScanner}.
   * Returns <code>null</code> if the key was not found.
   * @param scan of type {@link HFileScanner}
   * @param a the array whose string representation to return
   * @param offset the offset in a
   * @param length the length to print
   * @return a string representation of <tt>a</tt>, starting at the offset for length bytes.
   * @throws IOException if the search failed
   */
  private byte[] doGetValue(HFileScanner scan, byte[] key, int offset, int length)
  throws IOException {
    final int res = scan.seekTo(key, offset, length);
    if (res == -1) {
      if (scan.seekBefore(key, offset, length)) {
        while (scan.next()) {
          final KeyValue kv = scan.getKeyValue();
          if (WritableComparator.compareBytes(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength(),
              key, offset, length) == 0) {
            return kv.getValue();
          }
        }
      } else {
        if (scan.seekTo()) {
          final KeyValue kv = scan.getKeyValue();
          return WritableComparator.compareBytes(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength(),
            key, offset, length) == 0 ? kv.getValue() : null;
        }
      }
    } else if (res == 0) {
      final KeyValue kv = scan.getKeyValue();
      return kv.getValue();
    } else if (res == 1) {
      while (scan.next()) {
        final KeyValue kv = scan.getKeyValue();
        if (WritableComparator.compareBytes(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength(),
            key, offset, length) == 0) {
          return kv.getValue();
        }
      }
    }
    return null;
  }

  @Override
  public void loadDictionary(FlowProcess flowProcess, String dictionaryLabel)
  throws IOException {
    if (flowProcess instanceof HadoopFlowProcess) {
      final JobConf conf = ((HadoopFlowProcess) flowProcess).getJobConf();
      final FileSystem fs = FileSystem.getLocal(conf);

      if (AnalyticsUtil.isHadoopLocalMode(conf)) {
        localMode(conf, fs, dictionaryLabel);
      } else {
        hadoopMode(conf, fs, dictionaryLabel);
      }
    } else {
      throw new IllegalArgumentException("This dictionary cannot run with the flowProcess=" + flowProcess);
    }
  }

  /**
   * Load the dictionary data when the job is run on a hadoop cluster.
   * @param conf the {@link JobConf}
   * @param fs the {@link FileSystem}
   * @param dictionaryLabel the label of the dictionary
   * @throws IOException if the dictionary could not be loaded
   */
  private void hadoopMode(final JobConf conf, final FileSystem fs, final String dictionaryLabel)
  throws IOException {
    final List<HFileScanner> scanners = new ArrayList<HFileScanner>();
    Path[] files = DistributedCache.getLocalCacheFiles(conf);

    if (files == null) {
      throw new IllegalArgumentException("No dictionary found with identifying label=[" + dictionaryLabel + "]");
    } else {
      for (Path file : files) {
        if (!file.getName().startsWith(dictionaryLabel)) {
          continue;
        }

        logger.info("Hadoop run: Got HFile with prefix={} at {}", dictionaryLabel, file);
        final CacheConfig cacheConf = AnalyticsCacheConfig.registerBlockCache(conf, file);
        final HFile.Reader hfileReader = HFile.createReader(fs, file, cacheConf);
        scanners.add(hfileReader.getScanner(true, true));
      }
    }
    this.scanners = scanners.toArray(new HFileScanner[scanners.size()]);
  }

  /**
   * Load the dictionary data when the job is run on a local machine.
   * @param conf the {@link JobConf}
   * @param fs the {@link FileSystem}
   * @param dictionaryLabel the label of the dictionary
   * @throws IOException if the dictionary could not be loaded
   */
  private void localMode(final JobConf conf, final FileSystem fs, final String dictionaryLabel)
  throws IOException {
    final String p = conf.get(dictionaryLabel);
    if (p == null || p.isEmpty()) {
      return;
    }
    final Path path = new Path(p);

    final CacheConfig cacheConf = AnalyticsCacheConfig.registerBlockCache(conf, path);
    final HFile.Reader hfileReader = HFile.createReader(fs, path, cacheConf);

    logger.info("Local run: got HFile with prefix={} at {}", dictionaryLabel, path);
    scanners = new HFileScanner[] { hfileReader.getScanner(true, true) };
  }

  @Override
  public void close()
  throws IOException {
    try {
      in.close();
    } finally {
      try {
        out.close();
      } finally {
        if (scanners != null) {
          for (HFileScanner s : scanners) {
            if (s != null) {
              s.getReader().close(true);
            }
          }
        }
      }
    }
  }

}
