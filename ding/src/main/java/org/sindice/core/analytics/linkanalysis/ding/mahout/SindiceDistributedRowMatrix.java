/**
 * Copyright (c) 2009-2011 Sindice Limited. All Rights Reserved.
 *
 * Project and contact information: http://www.siren.sindice.com/
 *
 * This file is part of the SIREn project.
 *
 * SIREn is a free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * SIREn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with SIREn. If not, see <http://www.gnu.org/licenses/>.
 */
/**
 * @project graph-summary
 * @author Campinas Stephane [ 24 Nov 2011 ]
 * @link stephane.campinas@deri.org
 */
package org.sindice.core.analytics.linkanalysis.ding.mahout;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;
import org.apache.mahout.math.MatrixSlice;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import org.apache.mahout.math.hadoop.TransposeJob;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

/**
 * 
 */
public class SindiceDistributedRowMatrix extends DistributedRowMatrix {
  
  private final int numRows;
  private final int numCols;
  private boolean keepTempFiles;
  
  public SindiceDistributedRowMatrix(Path inputPath, Path outputTmpPath,
                                     int numRows, int numCols) {
    this(inputPath, outputTmpPath, numRows, numCols, false);
  }

  public SindiceDistributedRowMatrix(Path inputPath, Path outputTmpPath, int numRows, int numCols, boolean b) {
    super(inputPath, outputTmpPath, numRows, numCols, b);
    this.numRows = numRows;
    this.numCols = numCols;
    this.keepTempFiles = b;
  }

  @Override
  public Iterator<MatrixSlice> iterateAll() {
    try {
      return Iterators.transform(
          new SequenceFileDirIterator<IntWritable,VectorWritable>(getRowPath(),
                                                                  PathType.LIST,
                                                                  PathFilters.partFilter(),
                                                                  null,
                                                                  true,
                                                                  getConf()),
          new Function<Pair<IntWritable,VectorWritable>,MatrixSlice>() {
            @Override
            public MatrixSlice apply(Pair<IntWritable, VectorWritable> from) {
              return new MatrixSlice(from.getSecond().get(), from.getFirst().get());
            }
          });
    } catch (IOException ioe) {
      throw new IllegalStateException(ioe);
    }
  }
  
  @Override
  public SindiceDistributedRowMatrix transpose()
  throws IOException {
    Path outputPath = new Path(getRowPath().getParent(), "transpose-" + (System.nanoTime() & 0xFF));
    Configuration initialConf = getConf() == null ? new Configuration() : getConf();
    Configuration conf = TransposeJob.buildTransposeJobConf(initialConf, getRowPath(), outputPath, numRows);
    JobClient.runJob(new JobConf(conf));
    SindiceDistributedRowMatrix m = new SindiceDistributedRowMatrix(outputPath, getOutputTempPath(), numRows, numCols);
    m.setConf(conf);
    return m;
  }
  
  @Override
  public Vector times(Vector v) {
    try {
      Configuration initialConf = getConf() == null ? new Configuration() : getConf();
      final Path outputVectorTmpPath = new Path(getOutputTempPath(),
                                          new Path(Long.toString(System.nanoTime())));
      final Configuration conf =
          TimesSquaredJob.createTimesJobConf(initialConf,
                                             v,
                                             numRows,
                                             getRowPath(),
                                             outputVectorTmpPath);
      JobClient.runJob(new JobConf(conf));
      final Vector result = TimesSquaredJob.retrieveTimesSquaredOutputVector(conf);
      if (!keepTempFiles) {
        FileSystem fs = outputVectorTmpPath.getFileSystem(conf);
        fs.delete(outputVectorTmpPath, true);
      }
      return result;
    } catch (IOException ioe) {
      throw new IllegalStateException(ioe);
    }
  }
  
}
