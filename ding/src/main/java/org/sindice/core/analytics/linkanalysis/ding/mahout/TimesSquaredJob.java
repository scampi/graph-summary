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
import java.net.URI;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileValueIterator;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.Functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;

/**
 * 
 */
public class TimesSquaredJob {
  
  public static final String INPUT_VECTOR = "DistributedMatrix.times.inputVector";
  public static final String IS_SPARSE_OUTPUT = "DistributedMatrix.times.outputVector.sparse";
  public static final String OUTPUT_VECTOR_DIMENSION = "DistributedMatrix.times.output.dimension";

  public static final String OUTPUT_VECTOR_FILENAME = "DistributedMatrix.times.outputVector";

  private TimesSquaredJob() { }

  public static Configuration createTimesSquaredJobConf(Vector v, Path matrixInputPath, Path outputVectorPath)
    throws IOException {
    return createTimesSquaredJobConf(new Configuration(), v, matrixInputPath, outputVectorPath);
  }

  public static Configuration createTimesSquaredJobConf(Configuration initialConf,
                                                        Vector v,
                                                        Path matrixInputPath,
                                                        Path outputVectorPath) throws IOException {
    return createTimesSquaredJobConf(initialConf,
                                     v,
                                     matrixInputPath,
                                     outputVectorPath,
                                     TimesSquaredMapper.class,
                                     VectorSummingReducer.class);
  }

  public static Configuration createTimesJobConf(Vector v,
                                                 int outDim,
                                                 Path matrixInputPath,
                                                 Path outputVectorPath) throws IOException {
    return createTimesJobConf(new Configuration(), v, outDim, matrixInputPath, outputVectorPath);
  }

  public static Configuration createTimesJobConf(Configuration initialConf,
                                                 Vector v,
                                                 int outDim,
                                                 Path matrixInputPath,
                                                 Path outputVectorPath) throws IOException {
    return createTimesSquaredJobConf(initialConf,
                                     v,
                                     outDim,
                                     matrixInputPath,
                                     outputVectorPath,
                                     TimesMapper.class,
                                     VectorSummingReducer.class);
  }

  public static Configuration createTimesSquaredJobConf(Vector v,
                                                        Path matrixInputPath,
                                                        Path outputVectorPathBase,
                                                        Class<? extends TimesSquaredMapper> mapClass,
                                                        Class<? extends VectorSummingReducer> redClass)
    throws IOException {
    return createTimesSquaredJobConf(new Configuration(), v, matrixInputPath, outputVectorPathBase, mapClass, redClass);
  }

  public static Configuration createTimesSquaredJobConf(Configuration initialConf,
                                                        Vector v,
                                                        Path matrixInputPath,
                                                        Path outputVectorPathBase,
                                                        Class<? extends TimesSquaredMapper> mapClass,
                                                        Class<? extends VectorSummingReducer> redClass)
    throws IOException {
    return createTimesSquaredJobConf(initialConf,
                                     v,
                                     v.size(),
                                     matrixInputPath,
                                     outputVectorPathBase,
                                     mapClass,
                                     redClass);
  }

  public static Configuration createTimesSquaredJobConf(Vector v,
                                                        int outputVectorDim,
                                                        Path matrixInputPath,
                                                        Path outputVectorPathBase,
                                                        Class<? extends TimesSquaredMapper> mapClass,
                                                        Class<? extends VectorSummingReducer> redClass)
    throws IOException {

    return createTimesSquaredJobConf(new Configuration(),
                                     v,
                                     outputVectorDim,
                                     matrixInputPath,
                                     outputVectorPathBase,
                                     mapClass,
                                     redClass);
  }

  public static Configuration createTimesSquaredJobConf(Configuration initialConf,
                                                        Vector v,
                                                        int outputVectorDim,
                                                        Path matrixInputPath,
                                                        Path outputVectorPathBase,
                                                        Class<? extends TimesSquaredMapper> mapClass,
                                                        Class<? extends VectorSummingReducer> redClass)
    throws IOException {
    JobConf conf = new JobConf(initialConf, TimesSquaredJob.class);
    conf.setJobName("TimesSquaredJob: " + matrixInputPath);
    FileSystem fs = FileSystem.get(conf);
    matrixInputPath = fs.makeQualified(matrixInputPath);
    outputVectorPathBase = fs.makeQualified(outputVectorPathBase);

    long now = System.nanoTime();
    Path inputVectorPath = new Path(outputVectorPathBase, INPUT_VECTOR + '/' + now);
    SequenceFile.Writer inputVectorPathWriter = new SequenceFile.Writer(fs,
            conf, inputVectorPath, NullWritable.class, VectorWritable.class);
    Writable inputVW = new VectorWritable(v);
    inputVectorPathWriter.append(NullWritable.get(), inputVW);
    Closeables.close(inputVectorPathWriter, false);
    URI ivpURI = inputVectorPath.toUri();
    DistributedCache.setCacheFiles(new URI[] {ivpURI}, conf);

    conf.set(INPUT_VECTOR, ivpURI.toString());
    conf.setBoolean(IS_SPARSE_OUTPUT, !v.isDense());
    conf.setInt(OUTPUT_VECTOR_DIMENSION, outputVectorDim);
    org.apache.hadoop.mapred.FileInputFormat.addInputPath(conf, matrixInputPath);
    conf.setInputFormat(org.apache.hadoop.mapred.SequenceFileInputFormat.class);
    FileOutputFormat.setOutputPath(conf, new Path(outputVectorPathBase, OUTPUT_VECTOR_FILENAME));
    conf.setMapperClass(mapClass);
    conf.setMapOutputKeyClass(NullWritable.class);
    conf.setMapOutputValueClass(VectorWritable.class);
    conf.setReducerClass(redClass);
    conf.setCombinerClass(redClass);
    conf.setOutputFormat(org.apache.hadoop.mapred.SequenceFileOutputFormat.class);
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(VectorWritable.class);
    return conf;
  }

  public static Vector retrieveTimesSquaredOutputVector(Configuration conf) throws IOException {
    Path outputPath = FileOutputFormat.getOutputPath(new JobConf(conf));
    Path outputFile = new Path(outputPath, "part-00000");
    SequenceFileValueIterator<VectorWritable> iterator =
        new SequenceFileValueIterator<VectorWritable>(outputFile, true, conf);
    try {
      return iterator.next().get();
    } finally {
      Closeables.closeQuietly(iterator);
    }
  }

  public static class TimesSquaredMapper<T extends WritableComparable> extends MapReduceBase
      implements Mapper<T,VectorWritable, NullWritable,VectorWritable> {

    protected final Logger          logger                = LoggerFactory.getLogger(TimesSquaredJob.class);
    
    Vector outputVector;
    OutputCollector<NullWritable,VectorWritable> out;
    private Vector inputVector;
    
    @Override
    public void configure(JobConf conf) {
      try {
        URI[] localFiles = DistributedCache.getCacheFiles(conf);
        Preconditions.checkArgument(localFiles != null && localFiles.length >= 1,
                                    "missing paths from the DistributedCache");
        Path inputVectorPath = new Path(localFiles[0].getPath());

        SequenceFileValueIterator<VectorWritable> iterator =
            new SequenceFileValueIterator<VectorWritable>(inputVectorPath, true, conf);
        try {
          inputVector = iterator.next().get();
        } finally {
          Closeables.closeQuietly(iterator);
        }
        
        int outDim = conf.getInt(OUTPUT_VECTOR_DIMENSION, Integer.MAX_VALUE);
        outputVector = conf.getBoolean(IS_SPARSE_OUTPUT, false)
                       ? new RandomAccessSparseVector(outDim, 10)
                       : new DenseVector(outDim);
      } catch (IOException ioe) {
        throw new IllegalStateException(ioe);
      }
    }

    @Override
    public void map(T rowNum,
                    VectorWritable v,
                    OutputCollector<NullWritable,VectorWritable> out,
                    Reporter rep) throws IOException {
      this.out = out;
      
      final Vector vect = v.get();
      
      double d = scale(vect);
      if (d == 1.0) {
        outputVector.assign(vect, Functions.PLUS);
      } else if (d != 0.0) {
        outputVector.assign(vect, Functions.plusMult(d));
      }
    }
    
    protected double scale(Vector v) {
      return v.dot(inputVector);
    }

    @Override
    public void close() throws IOException {
      if (out != null) {
        out.collect(NullWritable.get(), new VectorWritable(outputVector));
      }
    }

  }

  public static class TimesMapper extends TimesSquaredMapper<IntWritable> {
    @Override
    public void map(IntWritable rowNum,
                    VectorWritable v,
                    OutputCollector<NullWritable,VectorWritable> out,
                    Reporter rep) {
      this.out = out;
      final Vector vect = v.get();
      double d = scale(vect);
      if (d != 0.0) {
        outputVector.setQuick(rowNum.get(), d);
      }
    }
  }

  public static class VectorSummingReducer extends MapReduceBase
      implements Reducer<NullWritable,VectorWritable,NullWritable,VectorWritable> {

    private Vector outputVector;
    private VectorWritable vectorWritable = new VectorWritable();
    
    @Override
    public void configure(JobConf conf) {
      int outputDimension = conf.getInt(OUTPUT_VECTOR_DIMENSION, Integer.MAX_VALUE);
      outputVector = conf.getBoolean(IS_SPARSE_OUTPUT, false)
                   ? new RandomAccessSparseVector(outputDimension, 10)
                   : new DenseVector(outputDimension);
    }

    @Override
    public void reduce(NullWritable n,
                       Iterator<VectorWritable> vectors,
                       OutputCollector<NullWritable,VectorWritable> out,
                       Reporter reporter) throws IOException {
      outputVector.assign(0);
      while (vectors.hasNext()) {
        VectorWritable v = vectors.next();
        if (v != null) {
          outputVector.assign(v.get(), Functions.PLUS);
        }
      }
      vectorWritable.set(outputVector);
      out.collect(NullWritable.get(), vectorWritable);
    }
  }
  
}
