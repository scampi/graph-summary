/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.testHelper.iotransformation;

import cascading.flow.FlowProcess;

/**
 * Base class for {@link FieldType}s implementations.
 */
public abstract class AbstractFieldType<T>
implements FieldType<T> {

  /** The content of the tested field */
  protected final String input;
  /** The {@link FlowProcess} used in this Cascading execution */
  protected final FlowProcess fp;

  /**
   * The input String to be converted.
   * @param fp of type {@link FlowProcess}
   * @param input the String to convert
   */
  public AbstractFieldType(FlowProcess fp, String input) {
    this.input = input;
    this.fp = fp;
  }

  @Override
  public final T convert() {
    if (input == null) {
      return null;
    }
    if (input.isEmpty()) {
      return getEmptyField();
    }
    return doConvert();
  }

  @SuppressWarnings("unchecked")
  @Override
  public final T sort(Object data) {
    return data == null ? null : doSort((T) data);
  }

  /**
   * Sort the data in object, as a preparation for JUnit tests assertions.
   * By default, it returns the object as is.
   * @param data the data to sort
   * @return the sorted data
   */
  protected T doSort(T data) {
    return (T) data;
  }

  /**
   * Decode and convert the input {@link String}.
   * @return the converted input {@link String} into an instance of type T
   */
  protected abstract T doConvert();

  /**
   * Return the <code>T</code> value corresponding to an empty field.
   */
  protected abstract T getEmptyField();

}
