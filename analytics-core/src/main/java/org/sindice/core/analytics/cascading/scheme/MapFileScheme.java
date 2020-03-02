/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.scheme;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.io.MapFile;

import cascading.scheme.Scheme;
import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * This {@link Scheme} creates a {@link MapFile}.
 * 
 * @deprecated use {@link HFileScheme}
 */
public class MapFileScheme
extends SequenceFile {

  protected Class<? extends Writable> keyType;
  protected Class<? extends Writable> valueType;
  protected ArchiveFormat             format;

  private static final long           serialVersionUID = 1L;

  public enum ArchiveFormat {
    ZIP, NONE
  }

  protected MapFileScheme() {
    super(null);
  }

  public MapFileScheme(Fields fields, Class<? extends Writable> keyType, Class<? extends Writable> valueType) {
    this(fields, keyType, valueType, ArchiveFormat.NONE);
  }

  public MapFileScheme(Fields fields,
                       Class<? extends Writable> keyType,
                       Class<? extends Writable> valueType,
                       ArchiveFormat format) {
    super(fields);
    if (fields.size() != 2) {
      throw new IllegalArgumentException("The fields must have only 2 arguments: " + fields);
    }
    this.keyType = keyType;
    this.valueType = valueType;
    if (keyType == null || valueType == null) {
      throw new IllegalArgumentException("Both fields must have a type");
    }
    this.format = format;
  }

  @Override
  public void sinkConfInit(FlowProcess<JobConf> flowProcess,
                           Tap<JobConf, RecordReader, OutputCollector> tap,
                           JobConf conf) {
    conf.setOutputKeyClass(keyType);
    conf.setOutputValueClass(valueType);
    switch (format) {
      case ZIP:
        conf.setOutputFormat(ZipMapFileOutputFormat.class);
        break;
      default:
        conf.setOutputFormat(MapFileOutputFormat.class);
    }
  }

  @Override
  public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Void, OutputCollector> sinkCall)
  throws IOException {
    Object keyValue = NullWritable.get();
    Object valueValue = NullWritable.get();

    keyValue = sinkCall.getOutgoingEntry().getObject(getSinkFields().get(0));
    valueValue = sinkCall.getOutgoingEntry().getObject(getSinkFields().get(1));

    sinkCall.getOutput().collect(keyValue, valueValue);
  }

  @Override
  public boolean source(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall)
  throws IOException {

    Tuple key = (Tuple) sourceCall.getContext()[0];
    Tuple value = (Tuple) sourceCall.getContext()[1];
    boolean result = sourceCall.getInput().next(key, value);
    if (!result) {
      return false;
    }
    Tuple tuple = sourceCall.getIncomingEntry().getTuple();

    if (sourceCall.getIncomingEntry().getFields().isDefined()) {
      tuple.setAll(key, value);
    } else {
      tuple.clear();
      tuple.addAll(key, value);
    }

    return true;
  }

}
