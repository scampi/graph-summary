/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.dictionary;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.sindice.core.analytics.util.Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;

/**
 * This class provides a caching mechanism for {@link MapFile.Reader}.
 * 
 * @deprecated use {@link HFileDictionary}
 */
public class MapFileCache<KEY extends WritableComparable, VALUE extends Writable> {

  private final Logger           logger           = LoggerFactory
                                                  .getLogger(MapFileCache.class);

  private final Configuration    conf             = new Configuration();
  private final MapFile.Reader   mfReader;
  private final Map<Long, VALUE> mfReaderCache;

  public static final int        MAX_RESULTS      = 10000;
  private static final float     HASH_LOAD_FACTOR = 0.75f;
  private final int              maxCacheSize;

  private final KEY              key;
  private final VALUE            val;
  private FlowProcess            flowProcess      = null;

  public enum Cache {
    MISS, HIT
  }

  /**
   * Initalize mapfile cache with default size
   * 
   * @param reader
   * @throws IOException
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  public MapFileCache(MapFile.Reader reader)
  throws IOException,
  InstantiationException,
  IllegalAccessException {
    this(reader, 8192 * 2);
  }

  /**
   * Initialize cache with user defined size
   * 
   * @param reader
   * @param maxCache
   * @throws IOException
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  public MapFileCache(MapFile.Reader reader, int maxCache)
  throws IOException,
  InstantiationException,
  IllegalAccessException {
    this.mfReader = reader;
    key = (KEY) reader.getKeyClass().newInstance();
    val = (VALUE) reader.getValueClass().newInstance();

    this.maxCacheSize = maxCache;
    final int hashCapacity = (int) Math.ceil(maxCacheSize / HASH_LOAD_FACTOR) + 1;
    mfReaderCache = new LinkedHashMap<Long, VALUE>(hashCapacity, .75F, true) {

      private static final long serialVersionUID = 1L;

      @Override
      protected boolean removeEldestEntry(Map.Entry<Long, VALUE> eldest) {
        return size() > maxCacheSize;
      }
    };
  }

  public void setFlowProcess(FlowProcess fp) {
    flowProcess = fp;
  }

  /**
   * Get the key of
   * 
   * @return
   */
  public KEY getKey() {
    return key;
  }

  /**
   * Get a single value from the mapfile
   */
  public VALUE getValue() {
    if (mfReader == null) {
      return val;
    }

    final String kName = key.toString();
    final long k = Hash.getHash64(kName);
    if (mfReaderCache.containsKey(k)) {
       if (flowProcess != null) {
        flowProcess.increment(Cache.HIT, 1);
      }
      return mfReaderCache.get(k);
    }
     if (flowProcess != null) {
      flowProcess.increment(Cache.MISS, 1);
    }
    try {
      if (mfReader.get(key, val) == null) {
        logger.error("no value found for the key=[{}]", kName);
        mfReaderCache.put(k, null);
        return null;
      }
      final VALUE v = (VALUE) val.getClass().newInstance();
      ReflectionUtils.copy(conf, val, v);
      mfReaderCache.put(k, v);
      return v;
    } catch (Exception e) {
      final StringWriter w = new StringWriter();
      e.printStackTrace(new PrintWriter(w));
      logger.error("Failed to retrieve key={}, got exception: {}", kName, w.toString());
      return null;
    }
  }

  /**
   * Get all keys and value that begin with a prefix from mapfile
   * 
   * @param prefix
   * @return
   */
  public Map<String, String> getKeysAndValuesWithPrefix(String prefix) {
    Map<String, String> results = new TreeMap<String, String>();
    Text vals = new Text();
    Text keys = new Text(prefix);

    int size = 0;

    try {
      keys = (Text) mfReader.getClosest(keys, vals);

      while (size < MAX_RESULTS && keys.toString().startsWith(prefix)) {
        results.put(keys.toString(), vals.toString());
        size++;
        mfReader.next(keys, vals);
      }

    } catch (IOException e) {}

    return results;
  }

  /**
   * Close mapfile
   * 
   * @throws IOException
   */
  public void close()
  throws IOException {
    mfReader.close();
  }

}
