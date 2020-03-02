/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading;

import java.lang.reflect.Field;

import org.sindice.core.analytics.cascading.ConfigurationKey;
import org.sindice.core.analytics.cascading.ConfigurationKey.Reset;
import org.sindice.graphsummary.cascading.properties.GetPropertiesGraph;

/**
 * This class defines parameters that are used in the Graph Summary computation.
 * <p>
 * Use the {@link ConfigurationKey} class to define the name and the default value of a parameter.
 */
public class SummaryParameters {

  /**
   * Enable the gathering of statistics about datatypes per predicate.
   * 
   * <p>
   * Default: <code>false</code>
   * 
   * @see GetPropertiesGraph
   */
  public final static ConfigurationKey<Boolean> DATATYPE           = ConfigurationKey
                                                                   .newInstance("datatype", new Reset<Boolean>() {
                                                                     @Override
                                                                     public Boolean reset() {
                                                                       return false;
                                                                     }
                                                                   });

  public static void reset() {
    for (Field field : SummaryParameters.class.getDeclaredFields()) {
      try {
        if (field.get(null) instanceof ConfigurationKey) {
          ConfigurationKey<?> c = (ConfigurationKey<?>) field.get(null);
          c.reset();
        }
      } catch (IllegalArgumentException e) {} catch (IllegalAccessException e) {}
    }
  }

}
