package org.sindice.core.analytics.stats.basic;

import java.util.ArrayList;

import org.openrdf.model.Statement;
import org.sindice.core.analytics.rdf.RDFParser;

import cascading.flow.hadoop.HadoopFlowProcess;

public class RDFHelper {

  /**
   * Return true if the set of statements contains at least 1 statement with
   * the given predicate/object pair.
   * <p>
   * The RDF data is in NQuads format.
   */
  public static boolean contains(final ArrayList<String> quads,
                                 final String predicate,
                                 final String object) {
    final RDFParser rdfParser = new RDFParser(new HadoopFlowProcess());

    for (String rdf: quads) {
      rdf = rdf.replaceFirst("\\s+\\.$", "");
      String triple = "";
      final int indContext = rdf.lastIndexOf(' ');
      if (indContext != -1) {
        triple = rdf.substring(0, indContext) + ".\n";
      }

      final Statement st = rdfParser.parseStatement(triple);
      if (st == null) {
        throw new RuntimeException("Unable to parse statement: " + rdf);
      }
      if (st.getPredicate().stringValue().equals(predicate) &&
          st.getObject().stringValue().equals(object)) {
        return true;
      }
    }
    return false;
  }

}
