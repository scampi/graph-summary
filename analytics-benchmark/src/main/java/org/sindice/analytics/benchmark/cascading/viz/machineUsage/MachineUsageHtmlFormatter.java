/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz.machineUsage;

import org.sindice.analytics.benchmark.cascading.viz.AbstractHtmlFormatter;


/**
 * This {@link FormatterType} exports results about the machine usage in a HTML table.
 */
public class MachineUsageHtmlFormatter
extends AbstractHtmlFormatter {

  @Override
  protected String getMeasurementsName() {
    return "Usage";
  }

  @Override
  protected String getCaption() {
    return "Machine usage";
  }
  
}
