/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.iotransformation;

import org.apache.hadoop.util.StringUtils;
import org.sindice.analytics.entity.AnalyticsUri;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
import org.sindice.core.analytics.rdf.AnalyticsClassAttributes;
import org.sindice.core.analytics.testHelper.iotransformation.AbstractFieldType;
import org.sindice.core.analytics.testHelper.iotransformation.FieldType;
import org.sindice.core.analytics.util.Hash;
import org.sindice.graphsummary.cascading.properties.GetPropertiesGraph;
import org.sindice.graphsummary.cascading.properties.TypesCount;

import cascading.flow.FlowProcess;

/**
 * This {@link FieldType} describes the field value of statistics about types associated to a cluster.
 * This field type is created by {@link GetPropertiesGraph}.
 * <p>
 * The definition of a type is on each line of the field. The definition is formatted following the regular
 * expression below:
 * <pre>&lt;TYPE&gt; &lt;CLASS-ATTRIBUTE&gt;=&lt;CARDINALITY&gt;(|&lt;CLASS-ATTRIBUTE&gt;=&lt;CARDINALITY&gt;)*</pre>
 * For example, the line below says that for the type <b>t1</b>, there are 2 class attributes <b>ca1</b> and <b>ca2</b>
 * such that the first former occurs 1 time, and the latter 2 times:
 * <pre>t1 ca1=1|ca2=2</pre>
 * 
 * @see AnalyticsClassAttributes
 */
public class TypesCountType
extends AbstractFieldType<TypesCount> {

  public TypesCountType(FlowProcess fp, String input) {
    super(fp, input);
  }

  @Override
  public TypesCount doConvert() {
    final TypesCount bs = new TypesCount();

    // Class Attributes
    final String caProp = fp.getStringProperty(AnalyticsParameters.CLASS_ATTRIBUTES_FIELD.toString());
    final String[] ca = StringUtils.getStrings(caProp);
    AnalyticsClassAttributes.initClassAttributes(ca == null ? AnalyticsParameters.CLASS_ATTRIBUTES_FIELD.get() : ca);

    final String[] types = input.split("\n");
    for (String type : types) {
      final int ws = type.indexOf(' ');
      if (ws == -1) {
        throw new IllegalArgumentException("Missing whitesapce ' ' character in [" + type + "]");
      }
      final long label = Hash.getHash64(type.substring(0, ws));
      final String[] classAttributes = type.substring(ws + 1).split("\\|");

      for (String classAttribute : classAttributes) {
        final int equal = classAttribute.indexOf('=');
        if (equal == -1) {
          throw new IllegalArgumentException("Missing equal '=' character in [" + classAttribute + "]");
        }
        final AnalyticsUri uri = new AnalyticsUri(classAttribute.substring(0, equal));
        final byte attrIndex = (byte) AnalyticsClassAttributes.CLASS_ATTRIBUTES.indexOf(uri);
        final long count = Long.valueOf(classAttribute.substring(equal + 1));
        bs.add(label, attrIndex, count);
      }
    }
    return bs;
  }

  @Override
  protected TypesCount getEmptyField() {
    return new TypesCount();
  }

}
