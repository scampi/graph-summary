/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.statistics;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import cascading.management.UnitOfWork;
import cascading.stats.CascadingStats;

/**
 * This {@link CountersHandler} exports the statistics saved by {@link StoreCountersHandler}
 * and {@link StoreOverviewCountersHandler} into latex tables.
 */
public class LatexCountersExportHandler
implements CountersHandler {

  private final Logger logger = LoggerFactory.getLogger(LatexCountersExportHandler.class);
  private Yaml         yaml   = new Yaml();
  private String       id;

  @Override
  public void process(UnitOfWork<? extends CascadingStats> work, File path)
  throws IOException {
    final Collection<File> yamlFiles = FileUtils
    .listFiles(path, new SuffixFileFilter(".yaml"), DirectoryFileFilter.DIRECTORY);

    for (final File f : yamlFiles) {
      BufferedReader buff = null;
      PrintWriter results = null;
      try {
        final String fileName = f.getName().replace(".yaml", ".tex");
        buff = new BufferedReader(new FileReader(
          new File(f.getParent(), f.getName())));
        results = new PrintWriter(new BufferedWriter(new FileWriter(
          new File(f.getParent(), fileName))));
        final Iterator<Object> resIt = yaml.loadAll(buff).iterator();

        writeDocumentHeader(results);
        while (resIt.hasNext()) {
          final Map<String, Object> stringObjectMap = (Map<String, Object>) resIt.next();
          final TreeMap<String, Object> sotreeMap = new TreeMap<String, Object>();
          sotreeMap.putAll(stringObjectMap);

          writeTableHeader(results);
          for (Entry<String, Object> c : sotreeMap.entrySet()) {
            if (c.getKey().equals(ID_FIELD)) {
              id = c.getValue().toString();
            } else {
              final Map<String, Object> stringValueMap = (Map<String, Object>) c.getValue();
              final TreeMap<String, Object> treeMap = new TreeMap<String, Object>();
              treeMap.putAll(stringValueMap);

              // Write the counters
              results.print("\\hline \\multirow{" + treeMap.size() + "}{*}{" +
                            escapeLatex(c.getKey()) + "}");
              for (Entry<String, Object> e : treeMap.entrySet()) {
                results.print(" & " + escapeLatex(e.getKey()) + " & ");
                results.println("\\numprint{" + e.getValue().toString() + "} \\\\");
              }
            }
          }
          closeTable(results);
        }
        closeDocument(results);
      } catch (IOException e) {
        logger.error("Yaml loading has failed", e);
      }
      finally {
        if (results != null) {
          results.close();
        }
        if (buff != null) {
          buff.close();
        }
      }
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
   * <p>
   * Write the header of the Latex file including package for formatting
   * purposes
   * </p>
   */
  private void writeDocumentHeader(PrintWriter p) {
    p.println("\\documentclass{article}");
    p.println("\\usepackage[utf8]{inputenc}");
    p.println("\\usepackage[T1]{fontenc}");
    p.println("\\usepackage{multirow}");
    p.println("\\usepackage{numprint}");
    p.println("\\usepackage{graphicx}");
    p.println("\\npthousandsep{,}");
    p.println("\\usepackage[section] {placeins}");
    p.println("\\usepackage{geometry}");

    p.println("\\usepackage[active,tightpage]{preview}");
    p.println("\\renewcommand{\\PreviewBorder}{1in}");
    p.println("\\newcommand{\\Newpage}{\\end{preview}\\begin{preview}}");

    p.println("\\begin{document}");
  }

  /**
   * <p>
   * Write the header of every table
   * </p>
   */
  private void writeTableHeader(PrintWriter p) {
    p.println("\\begin{table}");
    p.println("\\begin{preview}");
    p.println("\\centering");
    p.println("\\resizebox{\\textwidth}{!}{");
    p.println("\\begin{tabular}{|l|l|r|}");
  }

  /**
   * <p>
   * Write the footer of every tables
   * </p>
   */
  private void closeTable(PrintWriter p) {
    p.println("\\hline");
    p.println("\\end{tabular}");
    p.println("}");
    p.println("\\caption{" + escapeLatex(id) + "}");
    p.println("\\end{preview}");
    p.println("\\end{table}");
    p.println("\\clearpage");
  }

  /**
   * <p>
   * Write the footer of the Latex file
   * </p>
   */
  private void closeDocument(PrintWriter p) {
    p.println("\\end{document}");
  }

}
