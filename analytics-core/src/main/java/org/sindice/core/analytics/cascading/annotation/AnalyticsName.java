/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;

/**
 * This annotation defines the name of an {@link AnalyticsSubAssembly}.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface AnalyticsName {

  /**
   * The name of the annotated {@link AnalyticsSubAssembly}
   */
  String value();

}
