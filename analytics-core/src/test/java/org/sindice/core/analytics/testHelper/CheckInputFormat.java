/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.testHelper;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import cascading.tuple.Tuple;

/**
 * This {@link FileInputFormat} reads data from a file formatted
 * as described in {@link CheckScheme}. It returns a list of
 * {@link Tuple}s, where a tuple corresponds to one tuple definition.
 *
 * @see CheckScheme
 */
public class CheckInputFormat
extends FileInputFormat<LongWritable, Tuple> {

  @Override
  public RecordReader<LongWritable, Tuple> getRecordReader(InputSplit split,
                                                           JobConf job,
                                                           Reporter reporter)
  throws IOException {
    reporter.setStatus(split.toString());
    return new CheckRecordReader(job, (FileSplit) split);
  }

  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return false;
  }

}
