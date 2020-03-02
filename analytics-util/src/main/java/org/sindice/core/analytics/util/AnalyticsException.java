/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.util;

/**
 * Base class for all {@link Exception}s in the Analytics project.
 */
public class AnalyticsException extends RuntimeException {

  private static final long serialVersionUID = -540975184550254972L;

  /**
   * Constructs an <code>AnalyticsException</code> with no detail message.
   */
  public AnalyticsException() {
    super();
  }

  /**
   * Constructs an <code>AnalyticsException</code> with the specified detail
   * message.
   * 
   * @param message
   *          The detail message.
   */
  public AnalyticsException(final String message) {
    super(message);
  }

  /**
   * Constructs an <code>AnalyticsException</code> with the specified cause.
   * 
   * @param cause
   *          The cause.
   */
  public AnalyticsException(final Throwable cause) {
    super(cause);
  }

  /**
   * Constructs an <code>AnalyticsException</code> with the specified detail
   * message and cause.
   * 
   * @param message
   *          The detail message.
   * @param cause
   *          The cause.
   */
  public AnalyticsException(final String message, final Throwable cause) {
    super(message, cause);
  }

}
