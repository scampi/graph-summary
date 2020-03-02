/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.scheme;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.sindice.core.analytics.cascading.AnalyticsParameters;

/**
 * This {@link TextOutputFormat} appends the {@link AnalyticsParameters#EXTENSION} to the text output filename.
 */
public class ExtensionTextOutputFormat<K, V>
extends TextOutputFormat<K, V> {

  public ExtensionTextOutputFormat() {
  }

  @Override
  public RecordWriter<K, V> getRecordWriter(FileSystem fileSystem, JobConf conf, String name, Progressable progress)
  throws IOException {
    final String extension = conf.get(AnalyticsParameters.EXTENSION.toString());
    final String e = extension != null && !extension.isEmpty() ? "." + extension : "";
    return super.getRecordWriter(fileSystem, conf, name + e, progress);
  }

}
