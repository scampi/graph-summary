/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.testHelper.iotransformation;

import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;

import org.junit.Test;

import cascading.flow.FlowProcess;

/**
 * 
 */
public class TestFieldType {

  /**
   * Simple class for testing a {@link FieldType}.
   */
  public static class DogFieldType extends AbstractFieldType<String[]> {

    public DogFieldType(FlowProcess fp, String input) {
      super(fp, input);
    }

    @Override
    protected String[] doConvert() {
      return input.split(" ");
    }

    @Override
    public String[] doSort(String[] data) {
      Arrays.sort(data);
      return data;
    }

    @Override
    protected String[] getEmptyField() {
      return new String[] {};
    }

  }

  @Test
  public void testConvert() {
    final DogFieldType d = new DogFieldType(null, "grishka milou dingo cerbere");
    assertArrayEquals(new String[] { "grishka", "milou", "dingo", "cerbere" }, d.convert());
  }

  @Test
  public void testEmpty() {
    final DogFieldType d = new DogFieldType(null, "");
    assertArrayEquals(new String[] {}, d.convert());
    assertArrayEquals(new String[] {}, d.sort(d.convert()));
  }

  @Test
  public void testSort() {
    final DogFieldType d = new DogFieldType(null, "grishka milou dingo cerbere");
    final String[] expected = new String[] { "grishka", "milou", "dingo", "cerbere" };

    Arrays.sort(expected);
    assertArrayEquals(expected, d.sort(d.convert()));
  }

}
