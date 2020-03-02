/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz.schema;

import org.sindice.analytics.benchmark.cascading.viz.AbstractLatexFormatter;

/**
 * This {@link FormatterType} exports results about the schema precision error in a Latex table.
 */
public class SchemaLatexFormatter
extends AbstractLatexFormatter {

  @Override
  protected String getMeasurementsName() {
    return "Precision";
  }

  @Override
  protected String getCaption() {
    return "Schema precision";
  }

}
