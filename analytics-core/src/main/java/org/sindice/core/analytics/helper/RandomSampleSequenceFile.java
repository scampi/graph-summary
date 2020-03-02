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
 * This {@link RandomSampleSequenceFile} filters the {@link Tuple}s of a {@link SequenceFile} with a given probability.
 */
public class RandomSampleSequenceFile extends AbstractSampleSequenceFile {

  private final float probability;

  public RandomSampleSequenceFile(Path sfDir, String toSample, String regex, float p) throws IOException {
    super(sfDir, toSample, regex);
    probability = p;
  }

  @Override
  protected boolean writeTuple(TupleEntryCollector write, TupleEntry tuple, boolean isNewFile)
  throws IOException {
    if (Math.random() < probability) {
      write.add(tuple);
      return true;
    }
    return false;
  }

}
