/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz;

import java.lang.reflect.Constructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.scheme.Scheme;
import cascading.tuple.Fields;

/**
 * This is the base class for a {@link ResultsExporter} implementation.
 */
public abstract class AbstractResultsExporter
implements ResultsExporter {

  private static final Logger logger = LoggerFactory.getLogger(AbstractResultsExporter.class);

  /** The {@link Scheme} constructor that is used for reading the data */
  private Constructor<? extends Scheme> constructor = null;

  /**
   * This method sets the {@link Scheme} to use to read the input files.
   */
  @Override
  public void setSchemeClass(Class<? extends Scheme> schemeCLass) {
    if (schemeCLass == null) {
      throw new NullPointerException();
    }
    try {
      constructor = schemeCLass.getConstructor(Fields.class);
    } catch (Exception e) {
      logger.error("Unable to get Constructor for " + schemeCLass.getName(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Scheme getSchemeInstance(final Fields fields) {
    if (constructor == null) {
      return getDefaultSchemeInstance(fields);
    }
    try {
      return constructor.newInstance(fields);
    } catch (Exception e) {
      logger.error("Unable to create a new instance for " + constructor.toGenericString(), e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns the default {@link Scheme} instance for this {@link ResultsExporter}.
   */
  protected abstract Scheme getDefaultSchemeInstance(final Fields fields);

}
