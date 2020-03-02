/**
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
 */

package org.sindice.core.analytics.stats.util;

import java.util.ArrayList;
import java.util.List;

import org.sindice.core.analytics.rdf.RDFDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class RDFUtil {

  private static final Logger logger = LoggerFactory.getLogger(RDFUtil.class);

  private RDFUtil() {}

  /**
   * Returns a list of RDF format used in this document
   */
  public static List<String> getFormats(final RDFDocument doc) {
    final List<String> l = new ArrayList<String>();
    final Object f = doc.get(RDFDocument.FORMAT);

    if (f == null) {
      return l;
    }
    if (f instanceof String) {
      final String value = (String) f;
      if (!value
          .matches("sun\\.org\\.mozilla\\.javascript\\.internal\\.NativeArray@\\p{XDigit}+")) {
        l.add(value);
      }
    } else if (f instanceof List<?>) {
      l.addAll((List<String>) f);
    } else {
      logger.debug("Received unknown object instance: {}", f.getClass().toString());
    }
    return l;
  }

  /**
   * Returns true <b>ONLY</b> if the current document is in rdf/rdfa format, if
   * the document contains other formats or does not contain formats it returns
   * false.
   */
  public static boolean isRdfOrRdfa(RDFDocument doc) {
    List<String> formats = getFormats(doc);
    if (formats.isEmpty()) {
      return false;
    }
    for (String f : formats) {
      if (!(f.equalsIgnoreCase("rdf") || f.equalsIgnoreCase("rdfa"))) {
        return false;
      }
    }
    return true;
  }

}
