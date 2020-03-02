/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.helper;

import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.sindice.core.analytics.util.BytesRef;

/**
 * The {@link CatHFile} prints the content of an {@link HFile} to the standard output.
 */
public class CatHFile {

  /**
   * Prints the content of the given {@link HFile}
   * @param conf the {@link Configuration} through which the file is read
   * @param hfile the path to the {@link HFile}
   * @param keyClass the class of the {@link HFile} key
   */
  public void cat(final Configuration conf, Path hfile, String keyClass, boolean asBytes)
  throws Exception {
    final FileSystem fs = hfile.getFileSystem(conf);
    final CacheConfig cacheConf = new CacheConfig(conf);

    final HFile.Reader hfileReader = HFile.createReader(fs, hfile, cacheConf);
    final HFileScanner scanner = hfileReader.getScanner(false, false);

    try {
      if (!scanner.seekTo()) {
        System.out.println("Empty HFile");
        return;
      }
      do {
        if (!asBytes && keyClass != null) {
          Class<?> c = Class.forName(keyClass);
          if (c.isInstance(1L)) {
            System.out.println("key=" + scanner.getKey().getLong());
          } else {
            throw new RuntimeException("KeyClass=" + keyClass + " is not supported");
          }
        } else if (asBytes) {
          final ByteBuffer bbKey = scanner.getKey();
          final BytesRef key = new BytesRef(bbKey.array(), bbKey.arrayOffset(), bbKey.limit());
          System.out.println("key=" + key);
        } else {
          System.out.println("key=" + scanner.getKeyString());
        }

        if (asBytes) {
          final ByteBuffer bbValue = scanner.getValue();
          final BytesRef value = new BytesRef(bbValue.array(), bbValue.arrayOffset(), bbValue.limit());
          System.out.println("value=" + value);
        } else {
          System.out.println("value=" + scanner.getValueString());
        }
        System.out.println();
      } while (scanner.next());
    } finally {
      hfileReader.close(true);
    }
  }

}
