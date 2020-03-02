/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.helper;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


/**
 * The {@link CatMapFile} prints the content of {@link MapFile} to the standard output.
 */
public class CatMapFile {

  private final WritableComparable<?> key;
  private final Writable val;
  final MapFile.Reader reader;

  /**
   * Prints the content of the {@link MapFile} located in path
   * @param path the path to the {@link MapFile}
   */
  public CatMapFile(String path)
  throws Exception {
    final Configuration conf = new Configuration(true);
    reader = new MapFile.Reader(FileSystem.getLocal(conf),
        new Path(path).toString(), conf);
    key = (WritableComparable<?>) reader.getKeyClass().newInstance();
    val = (Writable) reader.getValueClass().newInstance();
  }

  public void cat() throws IOException {
    while (reader.next(key, val)) {
      System.out.println("key=" + key + "$");
      System.out.println("val=" + val + "$");
      System.out.println();
    }
    reader.close();
  }

}
