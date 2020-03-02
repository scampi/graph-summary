/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz.connectivity;

import org.sindice.analytics.benchmark.cascading.viz.AbstractHtmlFormatter;

/**
 * This {@link FormatterType} exports results about the connectivity precision error in a Html table.
 */
public class ConnectivityHtmlFormatter
extends AbstractHtmlFormatter {

  @Override
  protected String getMeasurementsName() {
    return "Precision";
  }

  @Override
  protected String getCaption() {
    return "Connectivity precision";
  }
  
}
