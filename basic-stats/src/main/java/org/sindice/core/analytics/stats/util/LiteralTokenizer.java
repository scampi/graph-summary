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
/**
 * @project statistics
 * @author Campinas Stephane [ 14 Dec 2011 ]
 * @link stephane.campinas@deri.org
 */
package org.sindice.core.analytics.stats.util;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.LengthFilter;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.openrdf.model.Literal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class LiteralTokenizer {

  private static final Logger logger = LoggerFactory
      .getLogger(LiteralTokenizer.class);

  private static TokenStream tokenStream = new StandardTokenizer(
      Version.LUCENE_30, new StringReader(""));

  public static Map<String, Integer> getTerms(Literal literal)
      throws IOException {
    return getTerms(literal.toString());
  }

  public static Map<String, Integer> getTerms(String literal)
      throws IOException {

    Map<String, Integer> terms = new HashMap<String, Integer>();

    ((StandardTokenizer) tokenStream).reset(new StringReader(literal
        .toString()));
    CharTermAttribute charTermAttribute = tokenStream
        .getAttribute(CharTermAttribute.class);
    TokenStream ts = (TokenStream) tokenStream;

    try {
      // normalise tokens
      ts = new LowerCaseFilter(Version.LUCENE_35, ts);
      ts = new StopFilter(Version.LUCENE_35, ts,
          StopAnalyzer.ENGLISH_STOP_WORDS_SET);
      ts = new LengthFilter(true, ts, 2, Integer.MAX_VALUE);

      // add a triple for each of the tokens
      while (ts.incrementToken()) {

        String currentToken = charTermAttribute.toString();

        if (terms.containsKey(currentToken)) {
          terms.put(currentToken, terms.get(currentToken) + 1);
        } else
          terms.put(currentToken, 1);
      }
    } catch (Exception e) {
      logger.error("Error while tokenizing literal: {}", literal);
    }
    return terms;
  }

}
