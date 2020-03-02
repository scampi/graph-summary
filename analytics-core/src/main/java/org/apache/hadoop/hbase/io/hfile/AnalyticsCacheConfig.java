/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.apache.hadoop.hbase.io.hfile;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.StoreFile;

/**
 * Extension of {@link CacheConfig} to allow for the caching of several {@link HFile}s.
 * <p>
 * See <a href="http://mail-archives.apache.org/mod_mbox/hbase-user/201205.mbox/%3C4FB8DEA0.3010003@deri.org%3E">post</a>
 */
public class AnalyticsCacheConfig
extends CacheConfig {

  private final static HashMap<Path, AnalyticsCacheConfig> blockCaches = new HashMap<Path, AnalyticsCacheConfig>();

  AnalyticsCacheConfig(BlockCache blockCache,
                       Configuration conf) {
    super(blockCache, DEFAULT_CACHE_DATA_ON_READ, DEFAULT_IN_MEMORY,
      conf.getBoolean(CACHE_BLOCKS_ON_WRITE_KEY, DEFAULT_CACHE_DATA_ON_WRITE),
      conf.getBoolean(CACHE_INDEX_BLOCKS_ON_WRITE_KEY, DEFAULT_CACHE_INDEXES_ON_WRITE),
      conf.getBoolean(CACHE_BLOOM_BLOCKS_ON_WRITE_KEY, DEFAULT_CACHE_BLOOMS_ON_WRITE),
      conf.getBoolean(EVICT_BLOCKS_ON_CLOSE_KEY, DEFAULT_EVICT_ON_CLOSE),
      conf.getBoolean(CACHE_DATA_BLOCKS_COMPRESSED_KEY, DEFAULT_COMPRESSED_CACHE));
  }

  public synchronized static CacheConfig registerBlockCache(Configuration conf, Path pathToDictionary) {
    if (!blockCaches.containsKey(pathToDictionary)) {
      /**
       * Copied from {@link CacheConfig} for the analytics use case
       */
      final float cachePercentage = conf.getFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, HConstants.HFILE_BLOCK_CACHE_SIZE_DEFAULT);
      if (cachePercentage == 0L) {
        return new CacheConfig(conf);
      }
      if (cachePercentage > 1.0) {
        throw new IllegalArgumentException(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY + " must be between 0.0 and 1.0, and not > 1.0");
      }

      // Calculate the amount of heap to give the heap.
      MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
      final long cacheSize = (long)(mu.getMax() * cachePercentage);
      final BlockCache blockCache = new LruBlockCache(cacheSize, StoreFile.DEFAULT_BLOCKSIZE_SMALL);
      final AnalyticsCacheConfig acc = new AnalyticsCacheConfig(blockCache, conf);
      blockCaches.put(pathToDictionary, acc);
      return acc;
    }
    return blockCaches.get(pathToDictionary);
  }

  public synchronized static void unRegisterAll() {
    blockCaches.clear();
  }

}
