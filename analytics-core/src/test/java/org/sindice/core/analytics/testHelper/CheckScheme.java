/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.testHelper;

import java.io.IOException;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.sindice.core.analytics.testHelper.iotransformation.FieldType;
import org.sindice.core.analytics.testHelper.iotransformation.IntType;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * This {@link Scheme} is to be used for tests purpose, and reads data from
 * a file with a specific format.
 *
 * <p>
 *
 * This scheme should be used only as a source.
 *
 * <p>
 *
 * A {@link CheckScheme} formatted file contains a list of tuple definitions.
 * A tuple definition represents the {@link Fields} content of a {@link Tuple}.
 * 
 * The start of a new tuple definition is marked with
 * {@link CheckRecordReader#TUPLES_START}.
 * 
 * The fields of a tuple definition are separated with
 * {@link CheckRecordReader#FIELDS_DELEM}.
 * 
 * The keyword {@link CheckRecordReader#NULL} indicates that the content of
 * a field is <code>null</code>.
 * 
 * <p>
 * 
 * The following example:
 * 
 * <pre>
 * ###
 * ste
 * cam
 * ---
 * c2
 * ###
 * c3
 * ---
 * NULL
 * </pre>
 * 
 * defines two tuples with two fields. The first one contains the tuple
 * <code>[ "ste\ncam", "c2" ]</code>. The second contains <code>[ "c3", null ]</code>.
 * 
 * <p>
 *
 * The number of fields of each tuple definition must be equal to the number
 * of fields passed in the constructor.
 * 
 * <p>
 * 
 * It is possible to set the {@link FieldType} of a field directly from the {@link CheckScheme}-formatted file.
 * The type definition of a field is found in the first line of the field's content, which line starts with the
 * characters {@link CheckRecordReader#FIELD_TYPE}. The rest of the line is the name of the class. This class
 * must extends {@link FieldType}. By default, the content of a field is a {@link String}.
 * For example, the file below sets the first field of the tuple as {@link IntType}.
 * <pre>
 * ###
 *  > org.sindice.core.analytics.testHelper.iotransformation.IntType
 * 42
 * ---
 * content of field 2 as a String
 * </pre>
 */
public class CheckScheme
extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Void> {

  private static final long serialVersionUID = -325648714776165627L;

  public CheckScheme(final Fields fields) {
    super(fields, fields);
  }

  @Override
  public void sourceConfInit(FlowProcess<JobConf> flowProcess,
                             Tap<JobConf, RecordReader, OutputCollector> tap,
                             JobConf conf) {
    conf.setInputFormat(CheckInputFormat.class);
  }

  @Override
  public void sinkConfInit(FlowProcess<JobConf> flowProcess,
                           Tap<JobConf, RecordReader, OutputCollector> tap,
                           JobConf conf) {}

  @Override
  public void sourcePrepare(FlowProcess<JobConf> flowProcess,
                            SourceCall<Object[], RecordReader> sourceCall) {
    final Object[] pair = new Object[] {
      sourceCall.getInput().createKey(),
      sourceCall.getInput().createValue()
    };

    sourceCall.setContext(pair);
  }

  @Override
  public boolean source(FlowProcess<JobConf> flowProcess,
                        SourceCall<Object[], RecordReader> sourceCall)
  throws IOException {
    final LongWritable key = (LongWritable) sourceCall.getContext()[0];
    final Tuple value = (Tuple) sourceCall.getContext()[1];
    boolean result = sourceCall.getInput().next(key, value);

    if (!result) {
      return false;
    }

    // todo: wrap tuples and defer the addAll
    Tuple tuple = sourceCall.getIncomingEntry().getTuple();
    if (value.size() != tuple.size()) {
      throw new RuntimeException("Expected fields " + getSourceFields() +
        " but got " + value.toString());
    }

    // key is always null/empty, so don't bother
    if (sourceCall.getIncomingEntry().getFields().isDefined()) {
      tuple.setAll(value);
    } else {
      tuple.clear();
      tuple.addAll(value);
    }

    return true;
  }

  @Override
  public void sourceCleanup(FlowProcess<JobConf> flowProcess,
                            SourceCall<Object[], RecordReader> sourceCall) {
    sourceCall.setContext(null);
  }

  @Override
  public void sink(FlowProcess<JobConf> flowProcess,
                   SinkCall<Void, OutputCollector> sinkCall)
  throws IOException {
    throw new NotImplementedException();
  }

}
