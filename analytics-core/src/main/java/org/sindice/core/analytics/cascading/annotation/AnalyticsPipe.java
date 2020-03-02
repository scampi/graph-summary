/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;

import cascading.pipe.Pipe;

/**
 * This annotation defines a {@link Pipe}, being either a head or a tail of an
 * {@link AnalyticsSubAssembly}.
 *
 * <p>
 *
 * A pipe is defined by its {@link #name()} and its set of {@link #fields()}.
 * If no name is defined, the name is then by default the {@link AnalyticsName#value() name}
 * of the annotated {@link AnalyticsSubAssembly}.
 *
 * <p>
 *
 * A head of the assembly can be defined as coming from the tail of another
 * {@link AnalyticsSubAssembly} by using {@link #from()}. In case where that assembly
 * has multiple tails, the {@link #sinkName()} of the desired tail must be defined.
 *
 * @see AnalyticsHeadPipes
 * @see AnalyticsTailPipes
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface AnalyticsPipe {

  /**
   * Returns the name of this pipe.
   */
  String name() default "";

  /**
   * Returns the fields' name
   */
  String[] fields() default {};

  /**
   * In the case where this pipe is a head, it returns the {@link AnalyticsSubAssembly}
   * that this assembly is sourcing from.
   */
  Class<? extends AnalyticsSubAssembly> from() default AnalyticsSubAssembly.class;

  /**
   * Used in conjunction with {@link #from()}, this method specifies the name of the tail this pipe reads from.
   */
  String sinkName() default "";

}
