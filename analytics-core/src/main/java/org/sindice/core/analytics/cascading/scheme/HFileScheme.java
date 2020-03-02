/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.scheme;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.sindice.core.analytics.cascading.AnalyticsParameters;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.hadoop.TupleSerialization.SerializationElementWriter;
import cascading.tuple.hadoop.io.HadoopTupleOutputStream;
import cascading.tuple.io.TupleOutputStream;

/**
 * This {@link Scheme} writes {@link HFile}s thanks to the {@link HFileOutputFormat}, which filename is prefixed with
 * the value of {@link AnalyticsParameters#HFILE_PREFIX}.
 * <p>
 * The class of the key must be supported by the {@link AnalyticsKeyComparator}.
 */
public class HFileScheme
extends Scheme<JobConf, RecordReader, OutputCollector, Void, HFileScheme.Context> {

  private static final long serialVersionUID = 6094481340181303597L;

  /** The {@link HFile} filename prefix, as per {@link AnalyticsParameters#HFILE_PREFIX} */
  private final String      prefix;
  /** Flag for indicating that the value is a {@link Tuple} */
  public static final byte  IS_TUPLE         = 0;
  /** Flag for indicating that the value is not a {@link Tuple} */
  public static final byte  IS_NOT_TUPLE     = 1;

  class Context {
    final TupleOutputStream out;
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    public Context(FlowProcess<JobConf> flowProcess) {
      final TupleSerialization ser = new TupleSerialization(flowProcess);
      SerializationElementWriter w = new SerializationElementWriter(ser);
      out = new HadoopTupleOutputStream(bytes, w);
    }
  }

  /**
   * Protected for use by TempDfs and other subclasses. Not for general
   * consumption.
   */
  protected HFileScheme() {
    super(null);
    prefix = AnalyticsParameters.HFILE_PREFIX.get();
  }

  /**
   * Creates a new SequenceFile instance that stores the given field names.
   * 
   * @param fields the declared {@link Fields}
   */
  public HFileScheme(Fields fields) {
    super(fields, fields);
    prefix = AnalyticsParameters.HFILE_PREFIX.get();
  }

  /**
   * Creates a new SequenceFile instance that stores the given field names.
   * 
   * @param fields the declared {@link Fields}
   * @param prefix the filename prefix of HFiles
   */
  public HFileScheme(Fields fields, String prefix) {
    super(fields, fields);
    this.prefix = prefix;
  }

  @Override
  public void sinkConfInit(FlowProcess<JobConf> flowProcess,
                           Tap<JobConf, RecordReader, OutputCollector> tap,
                           JobConf conf) {
    conf.setOutputKeyClass(byte[].class);
    conf.setOutputValueClass(byte[].class);
    conf.setOutputFormat(HFileOutputFormat.class);
    conf.set(AnalyticsParameters.HFILE_PREFIX.toString(), prefix);
  }

  @Override
  public void sinkPrepare(FlowProcess<JobConf> flowProcess, SinkCall<Context, OutputCollector> sinkCall)
  throws IOException {
    sinkCall.setContext(new Context(flowProcess));
  }

  @Override
  public void sink(FlowProcess<JobConf> flowProcess,
                   SinkCall<Context, OutputCollector> sinkCall)
  throws IOException {
    final Object keyValue = sinkCall.getOutgoingEntry().getObject(getSinkFields().get(0));
    final Object valueValue = sinkCall.getOutgoingEntry().getObject(getSinkFields().get(1));

    final Context c = sinkCall.getContext();

    c.bytes.reset();
    if (keyValue instanceof Tuple) {
      c.out.writeTuple((Tuple) keyValue);
    } else {
      c.out.writeElement(keyValue);
    }
    final byte[] key = c.bytes.toByteArray();


    c.bytes.reset();
    if (valueValue instanceof Tuple) {
      c.out.writeByte(IS_TUPLE);
      c.out.writeTuple((Tuple) valueValue);
    } else {
      c.out.writeByte(IS_NOT_TUPLE);
      c.out.writeElement(valueValue);
    }
    final byte[] value = c.bytes.toByteArray();

    sinkCall.getOutput().collect(key, value);
  }

  @Override
  public void sinkCleanup(FlowProcess<JobConf> flowProcess, SinkCall<Context, OutputCollector> sinkCall)
  throws IOException {
    sinkCall.getContext().bytes.reset();
    sinkCall.getContext().out.close();
  }

  @Override
  public boolean isSource() {
    return false;
  }

  @Override
  public void sourceConfInit(FlowProcess<JobConf> flowProcess,
                             Tap<JobConf, RecordReader, OutputCollector> tap,
                             JobConf conf) {
    throw new NotImplementedException();
  }

  @Override
  public boolean source(FlowProcess<JobConf> flowProcess, SourceCall<Void, RecordReader> sourceCall)
  throws IOException {
    throw new NotImplementedException();
  }

}
