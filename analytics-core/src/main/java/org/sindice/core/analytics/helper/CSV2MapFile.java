/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.helper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;

/**
 * Converts a CSV file into a {@link MapFile}.
 */
public class CSV2MapFile {

  public static boolean USE_GZIP = false;

  private final MapFile.Writer writer;
  private final File[] csv;

  private final Text key = new Text();
  private final Text value = new Text();

  private Integer[] keyFields;
  private Integer[] valueFields;

  private final StringBuilder sb = new StringBuilder();

  public CSV2MapFile(final File input, final String output) throws IOException {
    csv = input.listFiles(new FilenameFilter() {

      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith("part-");
      }
    });

    final Configuration conf = new Configuration();
    writer = new MapFile.Writer(conf, FileSystem.getLocal(conf), output,
        Text.class, Text.class, CompressionType.BLOCK);
  }

  public void setKeyFields(Integer[] fields) {
    keyFields = fields;
  }

  public void setValueFields(Integer[] fields) {
    valueFields = fields;
  }

  public void toMapFile(String sepRegex, String keySep, String valSep,
      boolean keyWithTrailing, boolean valWithTrailing) throws IOException {
    String line;
    String[] parts = null;
    final TreeMap<String, String> data = new TreeMap<String, String>();

    for (File f : csv) {
      final InputStream in = USE_GZIP ? new GZIPInputStream(
          new FileInputStream(f)) : new FileInputStream(f);
      final BufferedReader reader = new BufferedReader(new InputStreamReader(
          in));
      try {
        while ((line = reader.readLine()) != null) {
          try {
            parts = line.split(sepRegex);

            final String key = extractFields(keyFields, parts, keySep,
                keyWithTrailing);
            final String value = extractFields(valueFields, parts, valSep,
                valWithTrailing);
            data.put(key, value);
          } catch (Exception e) {
            System.err.println("line=" + line + " parts="
                + Arrays.toString(parts));
          }
        }
      } finally {
        reader.close();
      }
    }
    try {
      for (Entry<String, String> v : data.entrySet()) {
        key.set(v.getKey());
        value.set(v.getValue());
        writer.append(key, value);
      }
    } finally {
      writer.close();
    }
  }

  private String extractFields(Integer[] fields, String[] parts, String sep,
      boolean withTrailing) {
    sb.setLength(0);
    for (int i : fields) {
      if (!withTrailing) {
        sb.append(parts[i]);
      } else {
        sb.append(parts[i]).append(sep);
      }
    }
    return sb.toString();
  }

}
