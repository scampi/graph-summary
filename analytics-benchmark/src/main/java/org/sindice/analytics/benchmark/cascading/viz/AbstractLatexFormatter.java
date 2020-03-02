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
 * Base class for {@link Formatter}s that export results in a Latex table.
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
 * <p>
 * To compile the generated Latex table, it is necessary to include the following packages:
 * <ul>
 * <li>booktabs</li>
 * <li>multirow</li>
 * </ul>
 */
public abstract class AbstractLatexFormatter
implements Formatter {

  /** The rows for the results of this dataset */
  private final Map<String, Map<String, String>> rows = new TreeMap<String, Map<String, String>>();

  /** The set of algorithms' name */
  private Set<String>                            algorithms;

  @Override
  public void start(Writer out)
  throws IOException {
    algorithms = null;
    out.append("\\begin{table}\n")
       .append("  \\centering\n");
  }

  /**
   * Returns the column name for this {@link ResultsExporter}.
   */
  protected abstract String getMeasurementsName();

  @Override
  public void setAlgorithms(Writer out, Set<String> algorithms)
  throws IOException {
    if (this.algorithms == null) {
      this.algorithms = algorithms;
      // add the columns definition
      out.append("  \\begin{tabular}{lr");
      for (int i = 0; i < algorithms.size(); i++) {
        out.append("r");
      }
      out.append("}\n")
         .append("  \\toprule\n")
         .append("  Dataset & ").append(escapeLatex(getMeasurementsName()));
      // Add the column title
      for (String algo : algorithms) {
        out.append(" & ").append(escapeLatex(algo));
      }
      out.append(" \\\\\n")
         .append("  \\cmidrule{3-" + (2 + algorithms.size()) + "}\n");
    }
  }

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

    out.append("  \\multirow{" + rows.size() + "}{*}{" + escapeLatex(dataset) + "}");
    for (Entry<String, Map<String, String>> row : rows.entrySet()) {
      out.append(" & ").append(escapeLatex(row.getKey()));
      for (String algo : algorithms) {
        if (row.getValue().containsKey(algo)) {
          out.append(" & ").append(row.getValue().get(algo));
        } else {
          out.append(" & -");
        }
      }
      out.append(" \\\\\n");
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
   * Returns the table's caption.
   */
  protected abstract String getCaption();

  @Override
  public void end(Writer out)
  throws IOException {
    out.append("  \\bottomrule\n")
       .append("  \\end{tabular}\n")
       .append("  \\caption{").append(escapeLatex(getCaption())).append("}\n")
       .append("\\end{table}\n");
  }

}
