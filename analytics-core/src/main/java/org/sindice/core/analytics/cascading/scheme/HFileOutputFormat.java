/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.scheme;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.Writer;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
import org.sindice.core.analytics.util.AnalyticsUtil;

/**
 * This {@link FileOutputFormat} creates {@link HFile}s which filename is prefixed with the value of
 * {@link AnalyticsParameters#HFILE_PREFIX}, with a random appended number.
 * <p>
 * When run in {@link AnalyticsUtil local mode}, the created file has no value appended.
 */
public class HFileOutputFormat
extends FileOutputFormat<byte[], byte[]> {

  @Override
  public RecordWriter<byte[], byte[]> getRecordWriter(FileSystem ignored,
                                                      final JobConf conf,
                                                      String name,
                                                      Progressable progress)
  throws IOException {
    final Path outputPath = FileOutputFormat.getTaskOutputPath(conf, name);
    final FileSystem fs = outputPath.getFileSystem(conf);

    final int blocksize = conf.getInt("hbase.mapreduce.hfileoutputformat.blocksize", HFile.DEFAULT_BLOCKSIZE);
    final String compression = conf.get("hfile.compression", Compression.Algorithm.NONE.getName());
    final Random rand = new Random();

    return new RecordWriter<byte[], byte[]>() {
      // Map of families to writers and how much has been output on the
      // writer.
      private HFile.Writer writer = null;

      public void write(byte[] key, byte[] value)
      throws IOException {
        // create a new HLog writer, if necessary
        if (writer == null) {
          writer = getNewWriter(conf);
        }

        writer.append(key, value);
      }

      /**
       * Creates a new {@link Writer}.
       */
      private HFile.Writer getNewWriter(Configuration conf)
      throws IOException {
        final String prefix = conf.get(AnalyticsParameters.HFILE_PREFIX.toString(),
          AnalyticsParameters.HFILE_PREFIX.get());
        final String name;
        if (AnalyticsUtil.isHadoopLocalMode(conf)) {
          name = prefix;
        } else {
          name = prefix + ((System.identityHashCode(this) * 2654435761L ^ System.currentTimeMillis()) + rand.nextDouble());
        }
        writer = HFile.getWriterFactory(conf).createWriter(fs, new Path(outputPath.getParent(), name),
          blocksize, compression, new AnalyticsKeyComparator());
        return writer;
      }

      @Override
      public void close(Reporter reporter)
      throws IOException {
        if (writer != null) {
          writer.close();
        }
      }

    };
  }

}
