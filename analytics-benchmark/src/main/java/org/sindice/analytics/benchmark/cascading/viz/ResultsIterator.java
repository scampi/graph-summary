/**
 * Copyright (c) 2009-2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

/**
 * Iterates over the results files of a summarization algorithm on a dataset.
 * <p>
 * The files to be read are matched with the regular expression given by {@link #getResultsPattern()}.
 * The {@link Tuple} data that is read follow the {@link Fields} schema given by {@link #getInputFields()}.
 * The fields that a {@link ResultsIterator} implementation exposes to a consumer
 * is given by the {@link #getOutputFields()}.
 * <p>
 * If the {@link #getOutputFields()} differs from {@link #getInputFields()}, the method {@link #next()} must be
 * overwritten in order to exposes the correct data.
 */
public abstract class ResultsIterator
implements Iterator<TupleEntry>, Closeable {

  protected static final Logger logger = LoggerFactory.getLogger(ResultsIterator.class);

  private TupleEntryIterator it;

  /**
   * Returns the {@link Fields} expected by this {@link ResultsIterator} as input.
   */
  public abstract Fields getInputFields();

  /**
   * Returns the {@link Fields} this {@link ResultsIterator} exposes via {@link #next()}.
   */
  public abstract Fields getOutputFields();

  /**
   * Returns the regular expression that matches the file name of the files with the raw results.
   */
  public abstract String getResultsPattern();

  @Override
  public boolean hasNext() {
    return it.hasNext();
  }

  public void init(TupleEntryIterator it)
  throws IOException {
    this.it = it;
  }

  @Override
  public TupleEntry next() {
    return it.next();
  }

  public void close()
  throws IOException {
    it.close();
  }

  @Override
  public void remove() {
    throw new NotImplementedException();
  }

}
