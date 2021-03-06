/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.rdf;

import java.nio.charset.Charset;
import java.util.ArrayList;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFParser.DatatypeHandling;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.rio.ntriples.NTriplesParser;
import org.openrdf.sail.memory.model.MemValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RDF parsing utility class
 */
public class RDFParser2 {

  private static final Logger logger = LoggerFactory.getLogger(RDFParser2.class);

  private RDFParser2() {}

  private static final NTriplesParser np;
  private static final StatementCollector collector;
  private static final ReusableByteArrayInputStream bytes;

  static {
    np = new NTriplesParser(new MemValueFactory());
    collector = new StatementCollector();
    bytes = new ReusableByteArrayInputStream();

    np.setRDFHandler(collector);
    np.setPreserveBNodeIDs(true);
    np.setDatatypeHandling(DatatypeHandling.IGNORE);
    np.setVerifyData(false);
  }

  /**
   * Parses the NTriple statement.
   * Returns <code>null</code> if there was error parsing that statement.
   * @return the parsed {@link Statement}
   */
  public static synchronized Statement parseStatement(final String statement) {
    collector.clear();
    bytes.reset(statement.getBytes(Charset.forName("UTF-8")));
    try {
      np.parse(bytes, "");
      if (!collector.getStatements().isEmpty()) {
        return ((ArrayList<Statement>) collector.getStatements()).get(0);
      }
    } catch (Exception e) {
      logger.error("Error during RDF parsing {}: {}", statement, e.toString());
    }
    return null;
  }

}
