/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.helper;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.WritableSequenceFile;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/**
 * Base class for sampling {@link SequenceFile}s.
 */
public abstract class AbstractSampleSequenceFile {

  private final FileStatus[] sfDir;
  private final String       toSample;

  private String             keyClass;
  private String             valClass;

  public AbstractSampleSequenceFile(Path sfDir,
                                    String toSample,
                                    final String regex) throws IOException {
    final Configuration conf = new Configuration(true);
    this.sfDir = sfDir.getFileSystem(conf).listStatus(sfDir, new PathFilter() {

      @Override
      public boolean accept(Path path) {
        return path.getName().matches(regex);
      }

    });
    this.toSample = toSample;
  }

  public void sample(boolean isHadoop, int maxTuples)
  throws IOException, ClassNotFoundException {
    final JobConf conf = new JobConf(true);

    conf.set("mapred.output.compress", "true");
    conf.set("mapred.output.compression.type", "BLOCK");
    conf
    .set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

    HadoopFlowProcess flowProcess = new HadoopFlowProcess();

    final Class<? extends WritableComparable<?>> key;
    final Class<? extends Writable> val;
    if (isHadoop) {
      key = (Class<? extends WritableComparable<?>>) Class.forName(keyClass);
      val = (Class<? extends Writable>) Class.forName(valClass);
    } else {
      key = null;
      val = null;
    }

    final TupleEntryCollector write;
    if (isHadoop) {
      write = new Hfs(new WritableSequenceFile(new Fields("f1", "f2"), key, val), toSample)
      .openForWrite(flowProcess);
    } else {
      write = new Hfs(new SequenceFile(Fields.ALL), toSample)
      .openForWrite(flowProcess);
    }
    int nTuples = 0;
    try {
      for (FileStatus src : sfDir) {
        final TupleEntryIterator tuple;
        boolean isNewFile = true;

        if (isHadoop) {
          tuple = new Hfs(new WritableSequenceFile(new Fields("f1", "f2"), key, val), src
          .getPath().toString()).openForRead(flowProcess);
        } else {
          tuple = new Hfs(new SequenceFile(Fields.ALL), src.getPath()
          .toString()).openForRead(flowProcess);
        }

        try {
          while (tuple.hasNext()) {
            final TupleEntry entry = tuple.next();
            if (writeTuple(write, entry, isNewFile) && maxTuples != -1 &&
                ++nTuples >= maxTuples) {
              break;
            }
            isNewFile = false;
          }
        }
        finally {
          tuple.close();
        }
        if (maxTuples != -1 && nTuples >= maxTuples) {
          break;
        }
      }
    }
    finally {
      write.close();
    }
  }

  protected abstract boolean writeTuple(TupleEntryCollector write,
                                        TupleEntry tuple,
                                        boolean isNewFile)
  throws IOException;

  public void setKeyClass(String keyClass) {
    this.keyClass = keyClass;
  }

  public void setValClass(String valClass) {
    this.valClass = valClass;
  }

}
