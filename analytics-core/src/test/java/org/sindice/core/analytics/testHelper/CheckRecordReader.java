/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.testHelper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.sindice.core.analytics.testHelper.iotransformation.FieldType;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tuple.Tuple;

/**
 * This {@link RecordReader} parses a file formatted as described
 * in {@link CheckScheme}. The key is the position of the tuple definition,
 * and is a {@link LongWritable}. The value is that tuple definition
 * converted into a {@link Tuple}.
 *
 * @see CheckScheme
 */
public class CheckRecordReader
implements RecordReader<LongWritable, Tuple> {

  /** Mark the start of a new tuple */
  public static final String TUPLES_START = "###";
  /** Delimits the content of two fields */
  public static final String FIELDS_DELEM = "---";
  /** Keyword for indicating that the field's value is <code>null</code> */
  public static final String NULL = "NULL";
  /** Marks the explicit definition of the field's type */
  public static final String FIELD_TYPE = " > ";

  private final LinkedList<Tuple> tuples = new LinkedList<Tuple>();
  private int                     size;

  private final FlowProcess fp;

  public CheckRecordReader(final JobConf job,
                           final FileSplit split)
  throws IOException {
    final Path file = split.getPath();

    this.fp = new HadoopFlowProcess(job);
    final FileSystem fs = file.getFileSystem(job);
    final FSDataInputStream fileIn = fs.open(split.getPath());

    try {
      final BufferedReader r = new BufferedReader(new InputStreamReader(fileIn));
      final StringBuilder sb = new StringBuilder();
      String line = "";
      Tuple tuple = null;

      while ((line = r.readLine()) != null) { // for each tuple definition
        if (line.equals(TUPLES_START)) {
          if (tuple != null) {
            addField(tuple, sb);
            tuples.add(tuple);
          }
          tuple = new Tuple();
        } else if (line.equals(FIELDS_DELEM)) {
          addField(tuple, sb);
        } else {
          // to support content over multiple lines
          sb.append(line).append('\n');
        }
      }
      // Add the last tuple
      if (tuple != null) {
        addField(tuple, sb);
        tuples.add(tuple);
      }
      size = tuples.size();
    } finally {
      fileIn.close();
    }
  }

  /**
   * Add a field to the tuple. If the content is equal to {@value #NULL},
   * then the added field is <code>null</code>.
   */
  private void addField(final Tuple tuple,
                        final StringBuilder sb) {
    if (sb.length() != 0) {
      sb.deleteCharAt(sb.length() - 1); // delete the last newline
    }
    final String content = sb.toString();
    if (content.equals(NULL)) {
      tuple.add(null);
    } else if (content.startsWith(FIELD_TYPE)) {
      final int nl = content.indexOf('\n');
      if (nl == -1) {
        throw new IllegalArgumentException("The first line starting with \" > \" is used to" +
            " define the field's type definition. Missing new line character.");
      }
      try {
        Class<? extends FieldType<?>> clazz = (Class<? extends FieldType<?>>) Class.forName(sb.substring(3, nl));
        FieldType<?> ft = clazz.getConstructor(FlowProcess.class, String.class).newInstance(fp, sb.substring(nl + 1));
        tuple.add(ft.convert());
      } catch (Exception e) {
        throw new RuntimeException("Unable to get field type instance", e);
      }
    } else {
      tuple.add(content);
    }
    sb.setLength(0);
  }

  @Override
  public float getProgress() {
    if (size == 0) {
      return 0;
    }
    return (size - tuples.size()) / size;
  }

  @Override
  public void close()
  throws IOException {
  }

  @Override
  public boolean next(LongWritable key, Tuple value)
  throws IOException {
    if (tuples.isEmpty()) {
      return false;
    }
    key.set(size - tuples.size());
    value.clear();
    value.addAll(tuples.pollFirst());
    return true;
  }

  @Override
  public LongWritable createKey() {
    return new LongWritable();
  }

  @Override
  public Tuple createValue() {
    return new Tuple();
  }

  @Override
  public long getPos()
  throws IOException {
    return size - tuples.size();
  }

}
