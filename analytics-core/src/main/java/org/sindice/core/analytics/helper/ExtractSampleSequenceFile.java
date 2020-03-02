/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.helper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import cascading.scheme.hadoop.SequenceFile;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/**
 * The {@link ExtractSampleSequenceFile} retrieves from a {@link SequenceFile}s only the {@link Tuple}s which
 * {@link Fields} matche user-defined constraints.
 */
public class ExtractSampleSequenceFile extends AbstractSampleSequenceFile {

  private final Map<String, Filter> filters = new HashMap<String, Filter>();
  private final Map<String, Filter> fieldsOp = new HashMap<String, Filter>();

  public enum FilterEnum {
    MUST, MUST_NOT
  }

  public static class Filter {
    FilterEnum occur;
    String value;
  }

  public ExtractSampleSequenceFile(Path sfDir,
                                   String toSample,
                                   final String regex,
                                   Map<String, Filter> filters,
                                   Map<String, Filter> fieldsOp) throws IOException {
    super(sfDir, toSample, regex);
    this.fieldsOp.putAll(fieldsOp);
    this.filters.putAll(filters);
  }

  @Override
  protected boolean writeTuple(TupleEntryCollector write, TupleEntry tuple,
      boolean isNewFile) throws IOException {
    boolean extract = true;

    for (int pos = 0; pos < tuple.size(); pos++) { // for each field value
      final String fname = "fields" + pos;
      final String value = tuple.get(pos).toString();
      // field value filters
      if (filters.containsKey(fname)) {
        if (filters.get(fname).occur.equals(FilterEnum.MUST)
            && !filters.get(fname).value.equals(value)) {
          extract = false;
          break;
        } else if (filters.get(fname).occur.equals(FilterEnum.MUST_NOT)
            && filters.get(fname).value.equals(value)) {
          extract = false;
          break;
        }
      }
      // fields operators
      if (fieldsOp.containsKey(fname)) {
        if (fieldsOp.get(fname).occur.equals(FilterEnum.MUST)
            && !tuple.getString(fieldsOp.get(fname).value).equals(value)) {
          extract = false;
          break;
        } else if (fieldsOp.get(fname).occur.equals(FilterEnum.MUST_NOT)
            && tuple.getString(fieldsOp.get(fname).value).equals(value)) {
          extract = false;
          break;
        }
      }
    }
    if (extract) {
      write.add(tuple);
    }
    return false;
  }

}
