/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.annotation;

import java.util.LinkedHashMap;
import java.util.Map;

import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;

import riffle.process.Process;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

/**
 * This class provides methods for extracting information from the annotations
 * of an {@link AnalyticsSubAssembly} or a Riffle {@link Process}.
 *
 * @see AnalyticsName
 * @see AnalyticsHeadPipes
 * @see AnalyticsTailPipes
 */
@SuppressWarnings("rawtypes")
public class Analytics {

  private Analytics() {
  }

  /**
   * Returns the {@link AnalyticsName name} of the given
   * {@link AnalyticsSubAssembly} class.
   */
  public static String getName(final Class<?> analytics) {
    final AnalyticsName name = (AnalyticsName) analytics.getAnnotation(AnalyticsName.class);
    if (name == null) {
      throw new IllegalArgumentException("Unable to retrieve the pipe's name " +
          "using the annotation @AnalyticsName");
    }
    return name.value();
  }

  /**
   * Returns the {@link AnalyticsName name} of the given
   * {@link AnalyticsSubAssembly} instance.
   */
  public static String getName(final AnalyticsSubAssembly analytics) {
    return getName(analytics.getClass());
  }

  /**
   * Returns the {@link AnalyticsHeadPipes heads' fields} of the given
   * {@link AnalyticsSubAssembly} class.
   */
  public static Map<String, Fields> getHeadsFields(final Class<?> analytics) {
    final AnalyticsHeadPipes heads = (AnalyticsHeadPipes) analytics.getAnnotation(AnalyticsHeadPipes.class);
    if (heads == null) {
      throw new IllegalArgumentException("Unable to retrieve the pipe's heads " +
          "using the annotation @AnalyticsHeadPipes");
    }
    if (heads.values().length == 0) {
      throw new IllegalArgumentException("At least one head pipe must be defined" +
      " using the annotation @AnalyticsHeadPipes");
    }

    final Map<String, Fields> headFields = new LinkedHashMap<String, Fields>();
    final AnalyticsPipe[] values = heads.values();
    for (final AnalyticsPipe ap : values) {
      putAnalyticsPipe(analytics, ap, headFields, true, values.length);
    }
    if (headFields.isEmpty()) {
      throw new IllegalArgumentException("No head pipe defined");
    }
    return headFields;
  }

  /**
   * Returns the {@link AnalyticsHeadPipes heads' fields} of the given
   * {@link AnalyticsSubAssembly} instance.
   */
  public static Map<String, Fields> getHeadsFields(final AnalyticsSubAssembly analytics) {
    return getHeadsFields(analytics.getClass());
  }

  /**
   * Returns the {@link AnalyticsHeadPipes fields} of the head of the given
   * {@link AnalyticsSubAssembly} class, in the same order as they are written.
   */
  public static Fields getHeadFields(final Class analytics) {
    final Map<String, Fields> fields = getHeadsFields(analytics);
    return fields.values().iterator().next();
  }

  /**
   * Returns the {@link AnalyticsHeadPipes fields} of the head of the given
   * {@link AnalyticsSubAssembly} instance, in the same order as they are written
   */
  public static Fields getHeadFields(final AnalyticsSubAssembly analytics) {
    return getHeadFields(analytics.getClass());
  }

  /**
   * Returns the {@link AnalyticsHeadPipes pipes} of the head of the given
   * {@link AnalyticsSubAssembly} class.
   */
  public static Pipe[] getHeadsPipes(final Class analytics) {
    final Map<String, Fields> heads = getHeadsFields(analytics);
    final Pipe[] pipes = new Pipe[heads.size()];

    int i = 0;
    for (final String name: heads.keySet()) {
      pipes[i++] = new Pipe(name);
    }
    return pipes;
  }

  /**
   * Returns the {@link AnalyticsHeadPipes pipes} of the head of the given
   * {@link AnalyticsSubAssembly} instance.
   */
  public static Pipe[] getHeadsPipes(final AnalyticsSubAssembly analytics) {
    return getHeadsPipes(analytics.getClass());
  }

  /**
   * Returns the {@link AnalyticsTailPipes tails' fields} of the given
   * {@link AnalyticsSubAssembly} class, in the same order as they are written.
   */
  public static Map<String, Fields> getTailsFields(final Class<?> analytics) {
    final AnalyticsTailPipes tails = (AnalyticsTailPipes) analytics.getAnnotation(AnalyticsTailPipes.class);
    if (tails == null) {
      throw new IllegalArgumentException("Unable to retrieve the pipe's tails " +
          "using the annotation @AnalyticsTailPipes");
    }
    if (tails.values().length == 0) {
      throw new IllegalArgumentException("At least one tail pipe must be defined" +
      " using the annotation @AnalyticsTailPipes");
    }

    final Map<String, Fields> tailFields = new LinkedHashMap<String, Fields>();
    final AnalyticsPipe[] values = tails.values();
    for (final AnalyticsPipe ap : values) {
      putAnalyticsPipe(analytics, ap, tailFields, false, values.length);
    }
    if (tailFields.isEmpty()) {
      throw new IllegalArgumentException("No tail pipe defined");
    }
    return tailFields;
  }

  /**
   * Returns the {@link AnalyticsTailPipes tails' fields} of the given
   * {@link AnalyticsSubAssembly} instance, in the same order as they are written.
   */
  public static Map<String, Fields> getTailsFields(final AnalyticsSubAssembly analytics) {
    return getTailsFields(analytics.getClass());
  }

  /**
   * Returns the {@link AnalyticsTailPipes fields} of the tail of the given
   * {@link AnalyticsSubAssembly} class.
   */
  public static Fields getTailFields(final Class analytics) {
    final Map<String, Fields> fields = getTailsFields(analytics);
    return fields.values().iterator().next();
  }

  /**
   * Returns the {@link AnalyticsTailPipes fields} of the tail of the given
   * {@link AnalyticsSubAssembly} instance.
   */
  public static Fields getTailFields(final AnalyticsSubAssembly analytics) {
    return getTailFields(analytics.getClass());
  }

  /**
   * Returns the {@link AnalyticsTailPipes tails' pipes} of the given
   * {@link AnalyticsSubAssembly} class.
   */
  public static Pipe[] getTailsPipes(final Class analytics) {
    final Map<String, Fields> tails = getTailsFields(analytics);
    final Pipe[] pipes = new Pipe[tails.size()];

    int i = 0;
    for (final String name: tails.keySet()) {
      pipes[i++] = new Pipe(name);
    }
    return pipes;
  }

  /**
   * Returns the {@link AnalyticsTailPipes tails' pipes} of the given
   * {@link AnalyticsSubAssembly} instance.
   */
  public static Pipe[] getTailsPipes(final AnalyticsSubAssembly analytics) {
    return getTailsPipes(analytics.getClass());
  }

  /**
   * Extract the {@link Pipe} definition from the given {@link AnalyticsPipe}.
   *
   * @param analytics the annotated {@link AnalyticsSubAssembly} assembly or Riffle {@link Process}
   * @param ap the {@link AnalyticsPipe}
   * @param pipeFields the set of Pipes definitions, either heads or tails
   * @param isHead <code>true</code> if this ap defines a head
   * @param length the total number of pipes
   */
  private static void putAnalyticsPipe(final Class analytics,
                                       final AnalyticsPipe ap,
                                       final Map<String, Fields> pipeFields,
                                       final boolean isHead,
                                       final int length) {
    final String name;
    final Fields fields;

    /*
     * Get the name of the pipe
     */
    if (length == 1) { // if only one head/tail, its name is the name of the SubAssembly
      name = getName(analytics);
    } else if (ap.name().isEmpty() && ap.fields().length == 0) { // pipe coming from a specific sink
      name = ap.sinkName().isEmpty() ? getName(ap.from()) : ap.sinkName();
    } else if (ap.name().isEmpty()) {
      // head/tail pipe with no name. Take the name of this annotated pipe.
      name = getName(analytics);
    } else {
      name = ap.name();
    }
    /*
     * Get the fields of the pipe
     */
    if (ap.fields().length != 0) {
      fields = new Fields(ap.fields());
    } else if (isHead) {
      // The field configuration is defined by the tail pipes of #from().
      final Map<String, Fields> hf = getTailsFields(ap.from());
      final String sinkName = ap.sinkName();
      fields = hf.get(sinkName.isEmpty() ? getName(ap.from()) : sinkName);
      if (fields == null) {
        throw new IllegalArgumentException(analytics.getSimpleName() +
          ": Not properly defined head pipe with name " + name + ".");
      }
    } else {
      throw new IllegalArgumentException(analytics.getSimpleName() +
        ": Not properly defined " + (isHead ? "head" : "tail") + " pipe.");
    }
    if (pipeFields.containsKey(name)) {
      throw new IllegalArgumentException(analytics.getSimpleName() +
        ": Not properly defined " + (isHead ? "head" : "tail") + " pipe." +
        " The pipe " + name + " has already been defined.");
    }
    pipeFields.put(name, fields);
  }

}
