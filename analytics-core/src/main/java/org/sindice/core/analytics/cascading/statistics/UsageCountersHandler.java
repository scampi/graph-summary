/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.statistics;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.mapred.Task.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.management.UnitOfWork;
import cascading.stats.CascadingStats;

/**
 * This class extract the machine usage of an {@link UnitOfWork} and store them
 * into a Latex-formatted file as a bar plot.
 *
 * <p>
 *
 * The usage of a machine consists of the following metrics:
 * <ul>
 * <li>bytes read: HDFS_BYTES_READ and FILE_BYTES_READ;</li>
 * <li>bytes written: HDFS_BYTES_WRITTEN and FILE_BYTES_WRITTEN;</li>
 * <li>virtual memory: VIRTUAL_MEMORY_BYTES;</li>
 * <li>CPU time: CPU_MILLISECONDS; and</li>
 * <li>duration: the value of {@link CascadingStats#getDuration()}.</li>
 * </ul>
 */
public class UsageCountersHandler
implements CountersHandler {

  private final Logger logger = LoggerFactory.getLogger(StoreCountersHandler.class);
  private static final String FILE_NAME = "machine-usage.tex";

  /**
   * This class holds the machine usage relevant metrics
   */
  private class MachineUsage {
    // IO usage
    final long written; // HDFS + Local
    final long read; // HDFS + Local
    // Memory usage: virtual memory
    final long virtualMem;
    // CPU usage
    final long cpuTime;
    // Duration
    final long duration;

    public MachineUsage(CascadingStats stats) {
      read = stats.getCounterValue("FileSystemCounters", "HDFS_BYTES_READ")
             + stats.getCounterValue("FileSystemCounters", "FILE_BYTES_READ");
      written = stats.getCounterValue("FileSystemCounters", "HDFS_BYTES_WRITTEN")
                + stats.getCounterValue("FileSystemCounters", "FILE_BYTES_WRITTEN");
      virtualMem = stats.getCounterValue(Counter.class.getName(), "VIRTUAL_MEMORY_BYTES");
      cpuTime = stats.getCounterValue(Counter.class.getName(), "CPU_MILLISECONDS");
      duration = stats.getDuration();
    }
  }

  @Override
  public void process(final UnitOfWork<? extends CascadingStats> work,
                      final File path)
  throws IOException {
    final Collection<? extends CascadingStats> col = work.getStats().getChildren();

    if (col.size() > 1) {
      // Keep the order of the flow graph
      final Map<String, MachineUsage> childrenUsage = new LinkedHashMap<String, MachineUsage>();
      final Iterator<? extends CascadingStats> it = col.iterator();

      final File workDir = new File(path, work.getName().replace('/', '_'));
      workDir.mkdirs();

      logger.info("Bar plot of counters of the work: {}", work.getName());

      while (it.hasNext()) {
        final CascadingStats childStat = it.next();
        final String childName = childStat.getName().replace('/', '_');
        childrenUsage.put(childName, new MachineUsage(childStat));
      }

      final MachineUsage workUsage = new MachineUsage(work.getStats());
      createBarPlot(workUsage, childrenUsage, workDir);
    }
  }

  /**
   * Creates a bar plot from the machine usage statistics.
   *
   * <p>
   *
   * The statistics of each step of the work are grouped together by the metric.
   * @param workUsage
   * @param childrenUsage
   * @param workDir
   * @throws IOException
   */
  private void createBarPlot(final MachineUsage workUsage,
                             final Map<String, MachineUsage> childrenUsage,
                             final File workDir)
  throws IOException {
    final BufferedWriter usage = new BufferedWriter(new FileWriter(
      new File(workDir, FILE_NAME)));

    try {
      writeDocumentHeader(usage);
      writeBarHeader(usage);
      for (MachineUsage mu : childrenUsage.values()) {
        usage.append("\\addplot coordinates { ");
        addVertexAndNormalise(usage, "Written", mu.written, workUsage.written);
        addVertexAndNormalise(usage, "Read", mu.read, workUsage.read);
        addVertexAndNormalise(usage, "CPU", mu.cpuTime, workUsage.cpuTime);
        addVertexAndNormalise(usage, "Memory", mu.virtualMem, workUsage.virtualMem);
        addVertexAndNormalise(usage, "Duration", mu.duration, workUsage.duration);
        usage.append("};\n");
      }
      closeBar(usage, childrenUsage.keySet());
      closeDocument(usage);
    } finally {
      usage.close();
    }
  }

  /**
   * Adds a vertex to the bar plot, with the Y value normalised.
   * @param usage the {@link BufferedWriter} to write the vertex to
   * @param xLabel the X label of the vertex
   * @param a the Y value of the vertex
   * @param b the value to normalise Y with
   */
  private void addVertexAndNormalise(final BufferedWriter usage,
                                     final String xLabel,
                                     final long a,
                                     final long b)
  throws IOException {
    if (b == 0) {
      usage.append("(" + xLabel + ",0) ");
    } else {
      usage.append("(" + xLabel + "," + (a / (double) b) + ") ");
    }
  }

  /**
   * Escape characters that are interpreted by Latex.
   * @param s the original text
   * @return the escaped text
   */
  private String escapeLatex(String s) {
    return s.replace("_", "\\_").replace("#", "\\#").replace("$", "\\$");
  }

  /**
   * Write the header of the Latex file including package for formatting
   * purposes
   */
  private void writeDocumentHeader(BufferedWriter p)
  throws IOException {
    p.append("\\documentclass{article}\n");
    p.append("\\usepackage{pgf}\n");
    p.append("\\usepackage{tikz}\n");
    p.append("\\usepackage{pgfplots}\n");
    p.append("\\pgfplotsset{compat=newest}");
    p.append("\\usepackage{graphicx}\n");
    p.append("\\begin{document}\n");
  }

  /**
   * Open the bar plot environment, and configures the plot axis.
   */
  private void writeBarHeader(BufferedWriter p)
  throws IOException {
    p.append("\\begin{figure}\n" +
             "\\centering\n" +
             "\\begin{tikzpicture}\n" +
             " \\begin{axis}[\n" +
             "  symbolic x coords={Written,Read,CPU,Memory,Duration},\n" +
             "  xtick={Written,Read,CPU,Memory},\n" +
             "  xticklabel style={inner sep=0pt,anchor=north east,rotate=45},\n" +
             "  ylabel=Usage,\n" +
             "  enlargelimits=0.2,\n" +
             "  legend style={at={(0.5,-0.15)}, anchor=north,legend columns=3},\n" +
             "  ybar,\n" +
             "  bar width=7pt,\n" +
             "  width=1.4\textwidth" +
             " ]\n");
  }

  /**
   * Writes the legend of the bar plot, and closes the bar plot environment
   */
  private void closeBar(BufferedWriter p, Set<String> childrenName)
  throws IOException {
    final Iterator<String> it = childrenName.iterator();

    p.append("\\legend{");
    p.append(escapeLatex(it.next()));
    while (it.hasNext()) {
      p.append(',').append(escapeLatex(it.next()));
    }
    p.append("}\n\\end{axis}\n\\end{tikzpicture}\n" +
             "\\caption{Machine usage (Bytes written and read, CPU time, " +
             " Virtual memory, and Duration). The values have been normalised.}\n" +
             "\\end{figure}\n");
  }

  /**
   * Write the footer of the Latex file
   */
  private void closeDocument(BufferedWriter p)
  throws IOException {
    p.append("\\end{document}\n");
  }

}
