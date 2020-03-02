/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.scheme;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

/**
 * This {@link RecordReader} treats the filename as the key and the file's content as the value.
 */
class WholeFileRecordReader
implements RecordReader<Text, Text> {

  private FileSplit     fileSplit;
  private Configuration conf;
  private boolean       processed = false;

  public WholeFileRecordReader(FileSplit fileSplit, Configuration conf) throws IOException {
    this.fileSplit = fileSplit;
    this.conf = conf;
  }

  @Override
  public boolean next(Text key, Text value)
  throws IOException {
    if (!processed) {
      byte[] contents = new byte[(int) fileSplit.getLength()];
      Path file = fileSplit.getPath();

      String fileName = file.getName();
      key.set(fileName);

      FileSystem fs = file.getFileSystem(conf);
      FSDataInputStream in = null;
      try {
        in = fs.open(file);
        IOUtils.readFully(in, contents, 0, contents.length);
        value.set(contents, 0, contents.length);
      }
      finally {
        IOUtils.closeStream(in);
      }
      processed = true;
      return true;
    }
    return false;
  }

  @Override
  public Text createKey() {
    return new Text();
  }

  @Override
  public Text createValue() {
    return new Text();
  }

  @Override
  public long getPos()
  throws IOException {
    return processed ? fileSplit.getLength() : 0;
  }

  @Override
  public float getProgress()
  throws IOException {
    return processed ? 1.0f : 0.0f;
  }

  @Override
  public void close()
  throws IOException {
    // do nothing
  }

}
