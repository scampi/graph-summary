/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.viz;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

/**
 * Base class for {@link Formatter}s that export results in a HTML table.
 * <p>
 * The latex table this class produce follows the structure below:
 * <table border="1">
 *  <tr><th>Dataset</th><th> {@link #getMeasurementsName()} </th><th> Algo 1</th><th>...</th><th> Algo n</th></tr>
 *  <tr><td rowspan="2">Dataset A</td><td>Measurement a</td><td>Value Aa1</td><td>...</td><td>Value Aan</td></tr>
 *  <tr><td>Measurement b</td><td>Value Ab1</td><td>...</td><td>Value Abn</td></tr>
 *  <tr><td rowspan="2">Dataset B</td><td>Measurement a</td><td>Value Ba1</td><td>...</td><td>Value Ban</td></tr>
 *  <tr><td>Measurement b</td><td>Value Bb1</td><td>...</td><td>Value Bbn</td></tr>
 *  <caption> {@link #getCaption()} </caption>
 * </table>
 * <p>
 * The values of {@link #getMeasurementsName()} are the keys of {@link BenchmarkResults#getResults()}. The value of
 * the cell <b>Value Aa1</b> is the value stored in the {@link BenchmarkResults} which
 * {@link BenchmarkResults#getDataset()} is equal to <b>Dataset A</b>, {@link BenchmarkResults#getAlgorithm()} equals to
 * <b>Algo 1</b> and the key of {@link BenchmarkResults#getResults()} is <b>a</b>.
 */
public abstract class AbstractHtmlFormatter
implements Formatter {

  /** The rows for the results of this dataset */
  private final Map<String, Map<String, String>> rows  = new TreeMap<String, Map<String, String>>();

  /** The set of algorithms' name */
  private Set<String>                            algorithms;

  @Override
  public void start(Writer out)
  throws IOException {
    algorithms = null;
    out.append("<table border=\"1\" width=\"100%\">\n");
  }

  @Override
  public void setAlgorithms(Writer out, Set<String> algorithms)
  throws IOException {
    if (this.algorithms == null) {
      this.algorithms = algorithms;
      // add the columns definition
      out.append("  <tr><th>Dataset</th><th>").append(getMeasurementsName()).append("</th>");
      // Add the column title
      for (String algo : algorithms) {
        out.append("<th>").append(algo).append("</th>");
      }
      out.append("</tr>\n");
    }
  }

  /**
   * Returns the column name for this {@link ResultsExporter}.
   */
  protected abstract String getMeasurementsName();

  @Override
  public void addBenchmarkResults(Writer out, List<BenchmarkResults> brList)
  throws IOException {
    if (brList.isEmpty()) {
      return;
    }

    String dataset = null;
    rows.clear();
    for (BenchmarkResults br : brList) {
      dataset = br.getDataset();
      final String algo = br.getAlgorithm();
      for (Entry<String, String> entry : br.getResults().entrySet()) {
        final String k = entry.getKey();
        final String v = entry.getValue();
        if (!rows.containsKey(k)) {
          rows.put(k, new TreeMap<String, String>());
        }
        rows.get(k).put(algo, v);
      }
    }

    boolean first = true;
    out.append("  <tr><td rowspan=\"" + rows.size() + "\">" + dataset + "</td>");
    for (Entry<String, Map<String, String>> row : rows.entrySet()) {
      if (first) {
        first = false;
        out.append("<td>").append(row.getKey()).append("</td>");
      } else {
        out.append("<tr><td>").append(row.getKey()).append("</td>");
      }
      for (String algo : algorithms) {
        if (row.getValue().containsKey(algo)) {
          out.append("<td>").append(row.getValue().get(algo)).append("</td>");
        } else {
          out.append("<td>-</td>");
        }
      }
      out.append("</tr>\n");
    }
  }

  /**
   * Returns the table's caption.
   */
  protected abstract String getCaption();

  @Override
  public void end(Writer out)
  throws IOException {
    out.append("  <caption>").append(getCaption()).append("</caption>\n")
       .append("</table>\n");
  }

}
