/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.iotransformation;

import org.sindice.core.analytics.testHelper.iotransformation.AbstractFieldType;
import org.sindice.core.analytics.testHelper.iotransformation.FieldType;
import org.sindice.core.analytics.util.Hash;
import org.sindice.graphsummary.cascading.properties.PropertiesCount;

import cascading.flow.FlowProcess;

/**
 * This {@link FieldType} encodes the input data consisting of long pairs into a {@link PropertiesCount}.
 * <p>
 * The key is {@link Hash#getHash64(String) hashed} on 64 bits, and the value is converted to a long.
 * On each line of the field input, there is a key/value pair, where each follow the
 * formatting expression bellow:
 * <pre>&lt;KEY&gt;=&lt;VALUE&gt;</pre>
 * For example, if a property <b>p</b> occurs 10 times, it is written as follows:
 * <pre>p=10</pre>
 * <p>
 * It is possible to define the datatypes associated with the property by appending the datatype-count pair with the
 * same formatting. To the previous example, we can specify that the property has 2 datatypes, dt1 and dt2, as follows:
 * <pre>p=10 dt1=5 dt2=5</pre>
 * The sum of datatype count must sum up to the count of the property; an {@link IllegalArgumentException} is thrown
 * otherwise.
 */
public class PropertiesCountType
extends AbstractFieldType<PropertiesCount> {

  public PropertiesCountType(FlowProcess fp, String input) {
    super(fp, input);
  }

  @Override
  public PropertiesCount doConvert() {
    final PropertiesCount bs = new PropertiesCount();

    for (String prop : input.split("\n")) {
      final int equal = prop.indexOf('=');
      if (equal == -1) {
        throw new IllegalArgumentException("Missing equal '=' character in [" + prop + "]");
      }
      int space = prop.indexOf(' ', equal);
      final long p = Hash.getHash64(prop.substring(0, equal));
      final long pCount = Long.valueOf(prop.substring(equal + 1, space == -1 ? prop.length() : space));

      // add datatypes
      if (space != -1) {
        long actualCount = 0;

        while (space != -1) {
          final int prevSpace = space + 1;
          space = prop.indexOf(' ', prevSpace);
          final int equalDt = prop.indexOf('=', prevSpace);
          if (equalDt == -1) {
            throw new IllegalArgumentException("Missing colon ':' character for datatypes in [" + prop + "]");
          }
          final long dt = Hash.getHash64(prop.substring(prevSpace, equalDt));
          final long dtCount = Long.valueOf(prop.substring(equalDt + 1, space == -1 ? prop.length() : space));
          actualCount += dtCount;
          bs.add(p, dt, dtCount);
        }
        final long rest = pCount - actualCount;
        if (rest < 0) {
          throw new IllegalArgumentException("Invalid count of datatypes");
        } else if (rest > 0) { // the remaining has no datatype
          bs.add(p, PropertiesCount.NO_DATATYPE, rest);
        }
      } else {
        bs.add(p, PropertiesCount.NO_DATATYPE, pCount);
      }
    }
    return bs;
  }

  @Override
  protected PropertiesCount getEmptyField() {
    return new PropertiesCount();
  }

}
