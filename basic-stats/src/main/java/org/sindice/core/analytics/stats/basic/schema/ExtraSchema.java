/*******************************************************************************
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 *
 *
 * This project is a free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This project is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this project. If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.sindice.core.analytics.stats.basic.schema;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Compares the class names to find out the terms that do not belong to the original schema.org vocabulary
 * 
 * @author bibhas
 */
public class ExtraSchema {

  public final static int        size      = 750;
  public final static String     Table[]   = new String[size];

  public static LinkedList<Term> term_list = new LinkedList<Term>();
  public static int              list_size = 0;

  public static class Term {
    String word;
    int    freq;
    int    domain;

    public Term(String w, int d) {
      word = w;
      freq = 1;
      domain = d;
    }
  }

  /**
   * @throws IOException 
   * 
   */
  public ExtraSchema(String tableFileName)
  throws IOException {
    final BufferedReader tableReader = new BufferedReader(new InputStreamReader(new DataInputStream(new FileInputStream(tableFileName))));
    String line;
    int i = 0;

    try {
      while ((line = tableReader.readLine()) != null) {
        Table[i++] = line;
      }
    } finally {
      tableReader.close();
    }
  }

  public static Comparator<Term> MakeComparator = new Comparator<Term>() {

                                                  public int compare(Term A,
                                                                     Term B) {
                                                    if (A.freq < B.freq) {
                                                      return 1;
                                                    } else {
                                                      return -1;
                                                    }
                                                  }
                                                };

  public static int listSearch(String word) {
    int i;
    for (i = 0; i < term_list.size(); i++) {
      if (term_list.get(i).word.equalsIgnoreCase(word)) {
        return i;
      }
    }
    return -1;
  }

  public static int binarySearch(String word) {
    int mid = size / 2, low = 0, high = size - 1;
    while (low <= high) {
      mid = (low + high) / 2;
      if (Table[mid].equalsIgnoreCase(word)) {
        return mid;
      } else if (Table[mid].compareToIgnoreCase(word) < 0) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return -1;
  }

  public int compute(String schemaFileName,
                     String extraFileName)
  throws FileNotFoundException, IOException {
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.get(conf);
    final BufferedReader schemaReader = new BufferedReader(new InputStreamReader(fs.open(new Path(schemaFileName))));
    final BufferedWriter extraWriter = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(extraFileName), true)));

    String line;
    String word, wordToken;
    int index, domain;

    try {
      while ((line = schemaReader.readLine()) != null) {
        final StringTokenizer st = new StringTokenizer(line, "\t");
        word = st.nextToken();
        st.nextToken();
        st.nextToken();
        domain = Integer.parseInt(st.nextToken());
        index = word.indexOf('g');
        word = word.substring(index + 1);

        final StringTokenizer token = new StringTokenizer(word, "/");
        if (token.countTokens() != 0) {
          int pos = 0;
          while (token.hasMoreTokens()) {
            wordToken = token.nextToken();
            if (binarySearch(wordToken) == -1) {
              if ((pos = listSearch(wordToken)) == -1) {
                term_list.add(new Term(wordToken, domain));
              } else {
                Term t = term_list.get(pos);
                t.freq = t.freq + 1;
                t.domain = t.domain + domain;
              }
            }
          }
        }
      }
    } finally {
      schemaReader.close();
    }

    Collections.sort(term_list, MakeComparator);
    try {
      for (int i = 0; i < term_list.size(); i++) {
        extraWriter.append(term_list.get(i).word + "\t" + term_list.get(i).freq + "\t" +
                    term_list.get(i).domain).append('\n');
      }
    } finally {
      extraWriter.close();
    }
    return term_list.size();
  }

}
