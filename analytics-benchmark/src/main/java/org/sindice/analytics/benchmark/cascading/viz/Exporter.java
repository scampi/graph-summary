/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.sindice.analytics.benchmark.cascading.viz.Formatter.FormatterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;


/**
 * Exports benchmark results of a graph summarization approaches into a pretty and human-readable file format.
 * <p>
 * This class expects the benchmark results to be stored in the following directory structure:
 * <pre>&lt;dataset&gt;&#47;&lt;algorithm&gt;&#47;{regexp}</pre>
 * There are at the root of the results folder sub-folders with the dataset name. Inside each sub-folder, there are
 * folders with the algorithm name. Finally, the results of a benchmark are found within the latter folder. The raw 
 * results filenames must follow the regular expression given by {@link ResultsIterator#getResultsPattern()}.
 * <p>
 * Possible export formats are listed by {@link FormatterType}. There is one formatted file per {@link ResultsExporter},
 * and it is written at the root of the results folder. This file reports the benchmark results for all datasets and
 * algorithms found in the sub-folders.
 * 
 * @see ResultsExporter
 */
public class Exporter {

  private static final Logger logger = LoggerFactory.getLogger(Exporter.class);

  /**
   * Exports the benchmark results in the given directory using the provided set of {@link ResultsExporter exporters}.
   * @param ft the {@link FormatterType} to use
   * @param directory the directory that contains the benchmark results
   * @param outDir the directory where to write the data
   * @param fp the {@link FlowProcess}
   * @param exporters the array of {@link ResultsExporter} to apply on the benchmark results.
   * @throws IOException if an error occurs while writing the formatted benchmark results
   */
  public void export(final FormatterType ft,
                     final File directory,
                     final File outDir,
                     final FlowProcess fp,
                     final ResultsExporter... exporters)
  throws IOException {
    if (directory == null || outDir == null) {
      throw new NullPointerException();
    }
    final FileFilter dirFilter = new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.isDirectory();
      }
    };
    final File[] datasets = directory.listFiles(dirFilter);
    final List<BenchmarkResults> results = new ArrayList<BenchmarkResults>();

    Arrays.sort(datasets);
    for (ResultsExporter exporter : exporters) {
      final ResultsIterator resIt = exporter.getResultsIterator();
      final List<? extends ResultsProcessor> dps = exporter.getDatasetProcessor();
      final Formatter format = exporter.getFormatter(ft);
      final Writer out = new BufferedWriter(new FileWriter(new File(outDir, exporter.getName())));

      try {
        format.start(out);
        for (File dataset : datasets) {
          logger.info("Processing dataset: {}", dataset.getName());
          results.clear(); // process the results for this dataset

          final File[] algos = dataset.listFiles(dirFilter);
          Arrays.sort(algos);

          // Set the algorithms
          final Set<String> algosName = new TreeSet<String>();
          for (File file : algos) {
            algosName.add(file.getName());
          }
          format.setAlgorithms(out, algosName);

          for (File algo : algos) {
            logger.info("Processing algorithm: {}", algo.getName());
            process(resIt, dps, exporter, dataset, algo, fp, results);
          }
          // Format the data
          format.addBenchmarkResults(out, results);
        }
        format.end(out);
      } finally {
        out.close();
      }
    }
  }

  /**
   * This method process the results of an algorithm over a dataset.
   * @param resIt the {@link ResultsIterator} used to iterate over the data
   * @param dps the list of {@link ResultsProcessor} to apply on the results
   * @param exporter the current {@link ResultsExporter}
   * @param dataset the path to the dataset folder
   * @param algo the path to the algorithm folder
   * @param fp the {@link FlowProcess}
   * @param results the {@link List} of {@link BenchmarkResults} used to store
   *                the {@link ResultsProcessor#getProcessedResults() processed results}.
   * @throws IOException if an error occurs while processing the raw benchmark results
   */
  private void process(final ResultsIterator resIt,
                       final List<? extends ResultsProcessor> dps,
                       final ResultsExporter exporter,
                       final File dataset,
                       final File algo,
                       final FlowProcess fp,
                       final List<BenchmarkResults> results)
  throws IOException {
    // Iterate over the data
    final Fields inputFields = resIt.getInputFields();
    final String resPattern = resIt.getResultsPattern();
    final Scheme scheme = exporter.getSchemeInstance(inputFields);

    final File[] files = algo.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.matches(resPattern);
      }
    });
    if (files.length == 0) {
      logger.error("No file matching the regular expression [{}] was found in [{}]", resPattern, algo);
      return;
    }
    // Process the data
    for (ResultsProcessor dp : dps) {
      dp.reset();
      dp.setDataset(dataset.getName());
      dp.setAlgorithm(algo.getName());
      for (File data : files) {
        final Tap tap = new Hfs(scheme, data.getAbsolutePath());
        final TupleEntryIterator it = tap.openForRead(fp);

        try {
          resIt.init(it);
          dp.process(resIt);
        } finally {
          it.close();
        }
      }
      results.add(dp.getProcessedResults());
    }
  }

}
