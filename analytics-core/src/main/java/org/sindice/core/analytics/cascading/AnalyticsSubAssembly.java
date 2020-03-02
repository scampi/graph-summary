/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.IllegalClassException;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.annotation.AnalyticsHeadPipes;
import org.sindice.core.analytics.cascading.annotation.AnalyticsName;
import org.sindice.core.analytics.cascading.annotation.AnalyticsTailPipes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

/**
 * Base class for implementing a {@link SubAssembly} in the analytics projects.
 */
public abstract class AnalyticsSubAssembly
extends SubAssembly {

  private static final long        serialVersionUID = 1564898385363363820L;

  private static final Logger      logger           = LoggerFactory.getLogger(AnalyticsSubAssembly.class);

  /** The name of this assembly as per {@link AnalyticsName} */
  public final String              name             = Analytics.getName(this);
  /** The heads {@link Pipe}s of this {@link SubAssembly} */
  public final Map<String, Fields> heads            = Analytics.getHeadsFields(this);
  /** The tails {@link Pipe}s of this {@link SubAssembly} */
  public final Map<String, Fields> tails            = Analytics.getTailsFields(this);

  /**
   * Construct an {@link AnalyticsSubAssembly} instance
   * @param args arguments that can be accessed by the sub assembly.
   */
  public AnalyticsSubAssembly(final Object... args) {
    checkArgs(args);
    verifyAnnotations();
    final Pipe[] heads = Analytics.getHeadsPipes(this.getClass());
    final Pipe[] tailsPipes = getPipes(this.assemble(heads, args),
      Analytics.getTailsFields(this).keySet());
    final Pipe[] checkTail = Analytics.getTailsPipes(this.getClass());
    verifyPipes(checkTail, tailsPipes);
    this.setTails(tailsPipes);
  }

  /**
   * Construct an {@link AnalyticsSubAssembly} instance
   * @param headsPipes array of incoming {@link Pipe}s
   * @param args arguments that can be accessed by the sub assembly.
   */
  public AnalyticsSubAssembly(final Pipe[] headsPipes,
                              final Object... args) {
    checkArgs(args);
    verifyAnnotations();
    final Pipe[] checkHead = Analytics.getHeadsPipes(this.getClass());
    verifyPipes(checkHead, headsPipes);
    final Pipe[] tailsPipes = getPipes(this.assemble(headsPipes, args),
      Analytics.getTailsFields(this).keySet());
    final Pipe[] checkTail = Analytics.getTailsPipes(this.getClass());
    verifyPipes(checkTail, tailsPipes);
    this.setTails(tailsPipes);
  }

  /**
   * Asserts that each element of args implements {@link Serializable}.
   * A {@link RuntimeException} is thrown if not.
   */
  private void checkArgs(final Object[] args) {
    if (args == null) {
      return;
    }
    for (final Object o : args) {
      if (!(o instanceof Serializable)) {
        throw new RuntimeException("All assembly arguments must be serializable: got " +
            o.getClass().getName());
      }
    }
  }

  /**
   * Check that {@link Pipe} array meets the configuration specified in the
   * analytics annotations.
   * @param expected
   * @param actual
   */
  private void verifyPipes(final Pipe[] expected,
                           final Pipe[] actual) {
    if (actual.length != expected.length) {
      throw new IllegalArgumentException("Declared the pipes " + Arrays.toString(Pipe.names(expected)) +
        " but got " + Arrays.toString(Pipe.names(actual)));
    }
    for (int i = 0; i < expected.length; i++) {
      if (!expected[i].getName().equals(actual[i].getName())) {
        logger.warn("Expecting {} but got {}", Arrays.toString(Pipe.names(expected)),
          Arrays.toString(Pipe.names(actual)));
      }
    }
  }

  /**
   * Checks that the {@link AnalyticsSubAssembly} sub-class has the required annotations.
   */
  private void verifyAnnotations() {
    final Class<? extends AnalyticsSubAssembly> c = this.getClass();
    if (c.getAnnotation(AnalyticsName.class) == null) {
      throw new IllegalClassException("The class " + c.getSimpleName() + " misses" +
          " the @AnalyticsName annotation");
    }
    if (c.getAnnotation(AnalyticsHeadPipes.class) == null) {
      throw new IllegalClassException("The class " + c.getSimpleName() + " misses" +
          " the @AnalyticsHeadPipes annotation");
    }
    if (c.getAnnotation(AnalyticsTailPipes.class) == null) {
      throw new IllegalClassException("The class " + c.getSimpleName() + " misses" +
          " the @AnalyticsTailPipes annotation");
    }
  }

  /**
   * Returns the an array of {@link Pipe} from the {@link #assemble(Pipe..., Object...)} {@link Object}.
   */
  private Pipe[] getPipes(final Object pipes,
                          final Set<String> names) {
    final Pipe[] out;
    if (pipes instanceof Pipe) {
      out = Pipe.pipes((Pipe) pipes);
    } else if (pipes instanceof Pipe[]) {
      out = (Pipe[]) pipes;
    } else {
      throw new IllegalArgumentException("Expecting either a Pipe or an array" +
          " of Pipes, but #assemble returned " + pipes);
    }
    if (names.size() != out.length) {
      throw new IllegalArgumentException("Got " + Arrays.toString(Pipe.names(out)) +
        ", expecting " + names);
    }
    return out;
  }

  /**
   * Assemble a complex {@link Pipe}
   * @param previous the head pipes
   * @param args arguments that can be accessed by the sub assembly.
   * @return a {@link Pipe} or a {@link Pipe[]}
   */
  protected abstract Object assemble(Pipe[] previous, Object... args);

}
