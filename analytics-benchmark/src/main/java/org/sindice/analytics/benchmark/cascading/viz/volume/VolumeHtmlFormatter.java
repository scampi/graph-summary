/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz.volume;

import org.sindice.analytics.benchmark.cascading.viz.AbstractHtmlFormatter;


/**
 * This {@link FormatterType} exports results about the graph summary volume in a HTML table.
 */
public class VolumeHtmlFormatter
extends AbstractHtmlFormatter {

  @Override
  protected String getMeasurementsName() {
    return "Volume";
  }

  @Override
  protected String getCaption() {
    return "Volume of a graph summary";
  }
  
}
