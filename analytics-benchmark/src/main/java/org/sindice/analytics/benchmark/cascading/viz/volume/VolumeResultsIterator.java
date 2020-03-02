/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz.volume;

import org.sindice.analytics.benchmark.cascading.statistics.SummaryVolume;
import org.sindice.analytics.benchmark.cascading.statistics.SummaryVolumeCLI;
import org.sindice.analytics.benchmark.cascading.viz.ResultsIterator;
import org.sindice.core.analytics.cascading.annotation.Analytics;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * This {@link ResultsIterator} iterates over the {@link Tuple}s outputed by the {@link SummaryVolume} assembly.
 */
public class VolumeResultsIterator
extends ResultsIterator {

  @Override
  public Fields getInputFields() {
    return Analytics.getTailFields(SummaryVolume.class);
  }

  @Override
  public Fields getOutputFields() {
    return Analytics.getTailFields(SummaryVolume.class);
  }

  @Override
  public String getResultsPattern() {
    return SummaryVolumeCLI.VOLUME_OUT;
  }

}
