/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */

package org.sindice.core.analytics.cascading.statistics;

import java.io.File;

public class TableCountersExportHandlerCLI extends AbstractExportCLI{
  
  @Override
  protected void doRun(String input) throws Exception {
    File file = new File(input);
    LatexCountersExportHandler tce = new LatexCountersExportHandler();
    tce.process(null, file);
  }
  
  public static void main(String[] args) throws Exception {
    final TableCountersExportHandlerCLI cli = new TableCountersExportHandlerCLI();
    cli.run(args);
  }

}
