/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.helper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.Collection;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

/**
 * This class creates a sequence of {@link SequenceFile}s from some input data.
 * Each {@link SequenceFile} has a maximum file size. The input data can be gzip compressed.
 * Each line of the input data is written to the {@link SequenceFile}.
 * Compression can be enabled thanks via the {@link Tool} interface of {@link SplitToSequenceFileCLI}.
 */
public class SplitToSequenceFile {

  private final Logger  logger = LoggerFactory.getLogger(SplitToSequenceFile.class);

  /** The maximum file size, in bytes */
  private final long    maxFileSize;
  /** the {@link JobConf} to write the {@link SequenceFile} */
  private final JobConf conf;
  /** The number of the {@link SequenceFile} in the sequence */
  private int           nb     = 0;

  public SplitToSequenceFile(JobConf conf, final long maxSize)
  throws IOException {
    this.conf = conf;
    this.maxFileSize = maxSize;
  }

  /**
   * Create a new {@link Tap}
   * @param output the path to the folder
   * @return the {@link Hfs} instance with a {@link SequenceFile} {@link Scheme}
   */
  private Hfs getTap(final File output) {
    final String newOutput = new File(output, "split").getAbsolutePath();
    return new Hfs(new SequenceFile(new Fields("value")), newOutput, SinkMode.REPLACE);
  }

  /**
   * Get a {@link BufferedReader} over the input data.
   * Supports gzip compressed input files.
   * @param file the path to the file
   * @return the {@link BufferedReader} to read the lines of the input data
   * @throws Exception if an error occurred while opening the file
   */
  private BufferedReader getReader(File file)
  throws Exception {
    final Reader r;

    if (file.getName().endsWith(".gz")) {
      r = new InputStreamReader(new GZIPInputStream(new FileInputStream(file)));
    } else {
      r = new FileReader(file);
    }
    return new BufferedReader(r);
  }

  /**
   * Creates a sequence of {@link SequenceFile} with a fixed maximum size, given the input data folder.
   * The input folder is search recursively for files with the given extensions. If extensions is <code>null</code>,
   * all files are taken.
   * @param folder the path to the input folder
   * @param extensions the set of accepted extensions
   * @param output the path to the output folder where the {@link SequenceFile} will be written to
   * @throws Exception if an error occurred while reading or writing
   */
  public void split(final File folder, final String[] extensions, final File output)
  throws Exception {
    final Collection<File> files = FileUtils.listFiles(folder, extensions, true);
    logger.info("Found {} files with {} extension", files.size(),
      extensions == null ? "unknown" : Arrays.toString(extensions));
    final HadoopFlowProcess fp = new HadoopFlowProcess(conf);

    final Tuple tuple = Tuple.size(1);
    Hfs tap = getTap(output);
    TupleEntryCollector col = tap.openForWrite(fp);
    final File out = new File(tap.getIdentifier(), "part-00000");

    try {
      for (File file : files) {
        logger.info("Processing {}", file);
        final BufferedReader r = getReader(file);

        try {
          String line = null;
          while ((line = r.readLine()) != null) {
            tuple.set(0, line);
            if (out.length() >= maxFileSize) {
              col.close();

              // move split
              final String name = "split-" + String.format("%05d", nb++);
              FileUtils.moveFile(out, new File(out.getParentFile(), name));

              tap = getTap(output);
              col = tap.openForWrite(fp);
            }
            col.add(tuple);
          }
        } finally {
          r.close();
        }
      }
    } finally {
      col.close();
      // move split
      final String name = "split-" + String.format("%05d", nb++);
      FileUtils.moveFile(out, new File(output, name));
      FileUtils.deleteQuietly(out.getParentFile());
    }
  }

}
