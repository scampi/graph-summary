/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.dictionary;

import java.io.IOException;

import org.sindice.core.analytics.cascading.AnalyticsParameters;

import cascading.flow.FlowProcess;

/**
 * A factory for creating and loading a {@link Dictionary}.
 */
public class DictionaryFactory {

  /**
   * The type of {@link Dictionary} to instantiate.
   */
  public enum DictionaryType {
    HFILE, MAPFILE
  }

  private DictionaryFactory() {}

  /**
   * Returns a {@link Dictionary} instance with the type set by {@link AnalyticsParameters#DICTIONARY} in the given
   * {@link FlowProcess}. The dictionary data is identified by the dictionaryLabel string.
   * @param flowProcess of type {@link FlowProcess}
   * @param dictionaryLabel the dictionary identifier
   * @return a {@link Dictionary} loaded with the specified data
   * @throws IOException if the dictionary could not be created or loaded
   */
  public static Dictionary getDictionary(FlowProcess flowProcess, String dictionaryLabel)
  throws IOException {
    final String p = flowProcess.getStringProperty(AnalyticsParameters.DICTIONARY.toString());
    if (p != null) {
      AnalyticsParameters.DICTIONARY.set(DictionaryType.valueOf(p));
    }

    switch (AnalyticsParameters.DICTIONARY.get()) {
      case HFILE:
        final HFileDictionary hfile = new HFileDictionary(flowProcess);
        hfile.loadDictionary(flowProcess, dictionaryLabel);
        return hfile;
      default:
        throw new EnumConstantNotPresentException(DictionaryType.class,
          AnalyticsParameters.DICTIONARY.get().toString());
    }
  }

}
