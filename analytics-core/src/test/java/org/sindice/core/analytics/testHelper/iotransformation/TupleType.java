/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.testHelper.iotransformation;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import cascading.flow.FlowProcess;
import cascading.tuple.Tuple;

/**
 * This {@link FieldType} converts the input data into a {@link Tuple}, where each line of the input is an element
 * of the tuple. The elements can be further processed by setting the class of {@link FieldType} as the value
 * of the {@value #FIELD_TYPE} property via the {@link FlowProcess}. By default, {@link StringType} is applied.
 */
public class TupleType
extends AbstractFieldType<Tuple> {

  /** The parameter for setting the {@link FieldType} to apply on the elements */
  public static final String FIELD_TYPE = "tupleType.field.type";

  public TupleType(FlowProcess fp, String input) {
    super(fp, input);
  }

  @Override
  protected Tuple doConvert() {
    final String type = fp.getStringProperty(FIELD_TYPE);
    final String[] args = input.split("\n");

    if (type == null) {
      return new Tuple((Object[]) args);
    } else {
      try {
        final Tuple tuple = new Tuple();
        final Class<FieldType<?>> c = (Class<FieldType<?>>) Class.forName(type);
        final Constructor<FieldType<?>> cst = c.getConstructor(FlowProcess.class, String.class);

        for (String s : args) {
          FieldType<?> field = (FieldType) cst.newInstance(fp, s);
          tuple.add(field.convert());
        }
        return tuple;
      } catch (Exception e) {
        throw new IllegalArgumentException("Unable to create a [" + type + "] from [" + input + "]", e);
      }
    }
  }

  @Override
  protected Tuple doSort(Tuple data) {
    final List<Comparable> list = new ArrayList<Comparable>();

    for (int i = 0; i < data.size(); i++) {
      list.add((Comparable) data.getObject(i));
    }

    final Tuple t = new Tuple();
    Collections.sort(list);
    for (Comparable c : list) {
      t.add(c);
    }
    return t;
  }

  @Override
  protected Tuple getEmptyField() {
    return new Tuple();
  }

}
