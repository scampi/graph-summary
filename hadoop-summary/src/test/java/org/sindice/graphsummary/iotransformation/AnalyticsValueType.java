/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.iotransformation;

import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.rio.ntriples.NTriplesUtil;
import org.openrdf.sail.memory.model.MemValueFactory;
import org.sindice.analytics.entity.AnalyticsValue;
import org.sindice.core.analytics.testHelper.iotransformation.AbstractFieldType;

import cascading.flow.FlowProcess;

/**
 * Convert a N-Triple formatted string into an {@link AnalyticsValue}.
 */
public class AnalyticsValueType
extends AbstractFieldType<AnalyticsValue> {

  private final ValueFactory valueFactory;

  public AnalyticsValueType(FlowProcess fp, String s) {
    super(fp, s);
    valueFactory = new MemValueFactory();
  }

  @Override
  public AnalyticsValue doConvert() {
    final Value value = NTriplesUtil.parseValue(input, valueFactory);
    return AnalyticsValue.fromSesame(value);
  }

  @Override
  protected AnalyticsValue getEmptyField() {
    return null;
  }

}
