/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.testHelper.iotransformation;

/**
 * This interface outlines mechanisms for processing the content of tested fields.
 * 
 * @param <T> The class to convert the input string into
 * @see IOReabableCascadingTest
 */
public interface FieldType<T> {

  /**
   * Decode and convert the input {@link String} into the type T.
   * @return the converted input {@link String} into an instance of type T
   */
  public T convert();

  /**
   * Sort the data in object, as a preparation for JUnit tests assertions.
   * By default, it returns the object as is.
   * @param object the data to sort
   * @return the sorted data
   */
  public T sort(Object object);

}
