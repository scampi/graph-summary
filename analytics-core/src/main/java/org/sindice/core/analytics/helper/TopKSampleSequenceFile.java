/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.helper;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import cascading.scheme.hadoop.SequenceFile;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/**
 * The {@link TopKSampleSequenceFile} writes the topk {@link Tuple}s of the {@link SequenceFile}.
 */
public class TopKSampleSequenceFile
extends AbstractSampleSequenceFile {

  private final int topk;
  private int n;

  public TopKSampleSequenceFile(Path sfDir, String toSample, String regex,
      int topk) throws IOException {
    super(sfDir, toSample, regex);
    this.topk = topk;
  }

  @Override
  protected boolean writeTuple(TupleEntryCollector write, TupleEntry tuple,
      boolean isNewFile) throws IOException {
    if (isNewFile) {
      n = 0;
    }
    if (++n <= topk) {
      write.add(tuple);
      return true;
    }
    return false;
  }

}
