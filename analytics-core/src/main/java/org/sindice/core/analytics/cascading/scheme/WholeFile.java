/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.scheme;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * This {@link Scheme} puts the whole content of a file into a {@link Tuple}'s {@link Fields}.
 * @see WholeFileInputFormat
 * @see WholeFileRecordReader
 */
public class WholeFile
extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {

  private static final long serialVersionUID = -5942790137150572321L;

  public WholeFile(Fields fields) {
    super(fields);
    if (fields.size() != 1) {
      throw new IllegalArgumentException("Expected fields of size 1, got [" + fields + "]");
    }
  }

  @Override
  public void sourceConfInit(FlowProcess<JobConf> flowProcess,
                             Tap<JobConf, RecordReader, OutputCollector> tap,
                             JobConf conf) {
    final JobConf jc = (JobConf) conf;

    jc.setInputFormat(WholeFileInputFormat.class);
  }

  @Override
  public void sinkConfInit(FlowProcess<JobConf> flowProcess,
                           Tap<JobConf, RecordReader, OutputCollector> tap,
                           JobConf conf) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public void sourcePrepare(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) {
    sourceCall.setContext(new Object[2]);

    sourceCall.getContext()[0] = (Writable) sourceCall.getInput().createKey();
    sourceCall.getContext()[1] = (Writable) sourceCall.getInput().createValue();
  }

  @Override
  public boolean source(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall)
  throws IOException {
    Text key = (Text) sourceCall.getContext()[0];
    Text value = (Text) sourceCall.getContext()[1];
    boolean result = sourceCall.getInput().next(key, value);
    if (!result) {
      return false;
    }
    final Tuple tuple = sourceCall.getIncomingEntry().getTuple();

    tuple.clear();
    tuple.addAll(value.toString());
    return true;
  }

  @Override
  public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall)
  throws IOException {
    throw new UnsupportedOperationException("Not supported.");
  }

}
