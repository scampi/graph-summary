/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz.volume;

import org.sindice.analytics.benchmark.cascading.viz.AbstractLatexFormatter;

/**
 * This {@link FormatterType} exports results about the graph summary volume in a Latex table.
 */
public class VolumeLatexFormatter
extends AbstractLatexFormatter {

  @Override
  protected String getMeasurementsName() {
    return "Volume";
  }

  @Override
  protected String getCaption() {
    return "Volume of a graph summary";
  }
  
}
