/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz.machineUsage;

import org.sindice.analytics.benchmark.cascading.viz.AbstractLatexFormatter;

/**
 * This {@link FormatterType} exports results about the machine usage in a Latex table.
 */
public class MachineUsageLatexFormatter
extends AbstractLatexFormatter {

  @Override
  protected String getMeasurementsName() {
    return "Usage";
  }

  @Override
  protected String getCaption() {
    return "Machine usage";
  }
  
}
