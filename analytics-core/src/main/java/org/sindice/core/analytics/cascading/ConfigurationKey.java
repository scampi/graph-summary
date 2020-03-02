/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading;

/**
 * This class manages the value of a parameter.
 * <p>
 * Note: this will change with GL-102.
 * @param <T> the class of the value associated with this parameter
 */
public class ConfigurationKey<T> {

  private final String   name;
  private T              value;
  private final Reset<T> reset;

  public interface Reset<T> {
    public T reset();
  }

  private ConfigurationKey(String name, Reset<T> reset) {
    this.name = name;
    this.reset = reset;
    reset(); // initialise the default value
  }

  /**
   * Creates a new instance.
   * 
   * @param <T>
   *          the value's type
   * @return a new instance
   */
  public static <T> ConfigurationKey<T> newInstance(String name, Reset<T> reset) {
    return new ConfigurationKey<T>(name, reset);
  }

  public void reset() {
    set(reset.reset());
  }

  public T get() {
    return value;
  }

  public void set(T value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return name;
  }

}
