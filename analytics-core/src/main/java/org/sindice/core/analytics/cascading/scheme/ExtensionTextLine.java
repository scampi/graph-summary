/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.scheme;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.sindice.core.analytics.cascading.AnalyticsParameters;

import cascading.flow.FlowProcess;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;

public class ExtensionTextLine
extends TextLine {

  private static final long serialVersionUID = -8135191801655581959L;

  public ExtensionTextLine(String extension) {
    super();
    AnalyticsParameters.EXTENSION.set(extension);
  }

  public ExtensionTextLine(String extension, Compress sinkCompression) {
    super(sinkCompression);
    AnalyticsParameters.EXTENSION.set(extension);
  }

  public ExtensionTextLine() {
    super();
  }

  public ExtensionTextLine(Compress sinkCompression) {
    super(sinkCompression);
  }

  @Override
  public void sinkConfInit(FlowProcess<JobConf> flowProcess,
                           Tap<JobConf, RecordReader, OutputCollector> tap,
                           JobConf conf) {
    super.sinkConfInit(flowProcess, tap, conf);
    conf.set(AnalyticsParameters.EXTENSION.toString(), AnalyticsParameters.EXTENSION.get());
    conf.setOutputFormat(ExtensionTextOutputFormat.class);
  }

}
