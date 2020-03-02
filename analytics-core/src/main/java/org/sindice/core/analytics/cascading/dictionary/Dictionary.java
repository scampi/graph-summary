/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.dictionary;

import java.io.Closeable;
import java.io.IOException;

import cascading.flow.FlowProcess;

/**
 * A {@link Dictionary} gives a key to value mapping. The dictionary is loaded via a {@link FlowProcess}.
 * The data of the dictionary is identified by a dictionary label.
 */
public interface Dictionary extends Closeable {

  /**
   * Returns the value associated with the key, or <code>null</code> if it is not found.
   * @param key the key to search for
   * @return the associated value.
   */
  public Object getValue(Object key);

  /**
   * Load the dictionary from the {@link FlowProcess} that is identified by the given dictionaryLabel.
   * @param flowProcess the {@link FlowProcess}
   * @param dictionaryLabel the label identifying dictionary
   * @throws IOException if the dictionary cannot be loaded with the given {@link FlowProcess}
   */
  public void loadDictionary(FlowProcess flowProcess, String dictionaryLabel) throws IOException;

}
