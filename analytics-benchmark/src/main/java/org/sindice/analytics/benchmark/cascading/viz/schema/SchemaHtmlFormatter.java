/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz.schema;

import org.sindice.analytics.benchmark.cascading.viz.AbstractHtmlFormatter;

/**
 * This {@link FormatterType} exports results about the schema precision error in a HTML table.
 */
public class SchemaHtmlFormatter
extends AbstractHtmlFormatter {

  @Override
  protected String getMeasurementsName() {
    return "Precision";
  }

  @Override
  protected String getCaption() {
    return "Schema precision";
  }

}
