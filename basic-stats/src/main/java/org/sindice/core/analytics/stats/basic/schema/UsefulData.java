/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.stats.basic.schema;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

/**
 * It extracts from the original input file, the class/predicate names referencing schema.org.
 * 
 * @author bibhas
 */
public class UsefulData {

  public int compute(String inFileName, String outFileName)
  throws FileNotFoundException, IOException {
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.get(conf);
    final TupleEntryIterator it = new Hfs(new SequenceFile(Fields.ALL), inFileName).openForRead(new HadoopFlowProcess());
    final BufferedWriter w = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outFileName), true)));

    int count = 0;
    try {
      while (it.hasNext()) {
        final TupleEntry e = it.next();
        final String line = e.getTuple().toString();
        if (line.contains("schema.org")) {
          count++;
          w.append(line).append('\n');
        }
      }
    } finally {
      it.close();
      w.close();
    }
    return count;
  }

}
