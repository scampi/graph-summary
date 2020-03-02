/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.rdf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.codehaus.jackson.map.ObjectMapper;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.rio.RDFParser.DatatypeHandling;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.rio.ntriples.NTriplesParser;
import org.openrdf.rio.ntriples.NTriplesUtil;
import org.openrdf.sail.memory.model.MemValueFactory;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
import org.sindice.core.analytics.util.AnalyticsCounters;
import org.sindice.core.analytics.util.AnalyticsException;
import org.sindice.core.analytics.util.ReusableStringReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * RDF parsing utility class
 */
public class RDFParser {

  private static final Logger logger = LoggerFactory.getLogger(RDFParser.class);

  // Parser data
  private final MemValueFactory vFactory = new MemValueFactory();
  private final NTriplesParser np = new NTriplesParser(vFactory);
  private final StatementCollector collector = new StatementCollector();
  private final ReusableStringReader stringReader = new ReusableStringReader();

  private final ObjectMapper mapper = new ObjectMapper();
  private final RDFDocument doc;
  private final FlowProcess fp;

  /**
   * Create a new {@link RDFParser} instance
   * @param fp the {@link FlowProcess} of this execution
   */
  public RDFParser(final FlowProcess fp) {
    // Initialise parameters
    AnalyticsParameters.DEFAULT_DOMAIN.set(fp.getStringProperty(AnalyticsParameters.DEFAULT_DOMAIN.toString()));
    final String documentFormat;
    if ((documentFormat = fp.getStringProperty(AnalyticsParameters.DOCUMENT_FORMAT.toString())) != null) {
      AnalyticsParameters.DOCUMENT_FORMAT.set(DocumentFormat.valueOf(documentFormat));
    }

    np.setRDFHandler(collector);
    np.setPreserveBNodeIDs(true);
    np.setDatatypeHandling(DatatypeHandling.IGNORE);
    np.setVerifyData(false);

    doc = new RDFDocument(this);
    this.fp = fp;
  }

  /**
   * Returns the {@link ValueFactory} held by this instance.
   */
  public ValueFactory getValueFactory() {
    return vFactory;
  }

  /**
   * Releases any resources hold by this instance.
   */
  public void clean() {
    vFactory.clear();
    collector.clear();
    try {
      stringReader.close();
    } catch (IOException e) {
      // ignore
    }
  }

  /**
   * Returns the String value of a {@link Value}.
   * @deprecated Use {@link Value#stringValue()}
   */
  public static String getStringValue(final Value v) {
    if (v == null) {
      return null;
    }
    if (v instanceof Literal || v instanceof BNode) {
      return v.stringValue();
    } else {
      return v.toString();
    }
  }

  /**
   * Parses the NTriple statement.
   * Returns <code>null</code> if there was error parsing that statement, in which
   * case the counter <b> {@link AnalyticsCounters#ERROR} + "RDF_PARSE"<b> in the group
   * {@link AnalyticsCounters#GROUP} is incremented.
   * @return the parsed {@link Statement}
   */
  public Statement parseStatement(final String statement) {
    collector.clear();
    stringReader.setValue(statement);
    try {
      np.parse(stringReader, "");
      if (!collector.getStatements().isEmpty()) {
        return ((ArrayList<Statement>) collector.getStatements()).get(0);
      }
    } catch (Exception e) {
      logger.error("Error during RDF parsing {}: {}", statement, e.toString());
      fp.increment(AnalyticsCounters.GROUP, AnalyticsCounters.ERROR + "RDF_PARSE", 1);
    }
    return null;
  }

  /**
   * Returns the {@link RDFDocument} stored in the {@link TupleEntry}.
   * Supports 3 formats:
   * <ul>
   * <li>Sindice JSON document</li>
   * <li>NTriple</li>
   * <li>NQuad</li>
   * </ul>
   * @param args The {@link TupleEntry} with the RDF data
   * @throws AnalyticsException if a problem happened while processing the {@link TupleEntry}
   */
  public RDFDocument getRDFDocument(final TupleEntry args)
  throws AnalyticsException {
    if (args.size() != 1) {
      throw new AnalyticsException("Expected tuple with 1 field, got: " + args.getFields());
    }

    doc.clear();
    switch (AnalyticsParameters.DOCUMENT_FORMAT.get()) {
      case SINDICE_EXPORT:
        parseJson(args);
        break;
      case NTRIPLES:
        parseNtriples(args);
        break;
      case NQUADS:
        parseNQuads(args);
        break;
      default:
        throw new EnumConstantNotPresentException(DocumentFormat.class,
          AnalyticsParameters.DOCUMENT_FORMAT.get().toString());
    }

    // Fills the dataset label value
    final String datasetLabel;
    if (AnalyticsParameters.DEFAULT_DOMAIN.get() != null) {
      datasetLabel = AnalyticsParameters.DEFAULT_DOMAIN.get();
    } else {
      try {
        datasetLabel = doc.getSndDomain();
      } catch (AnalyticsException e) {
        logger.info("Unable to extract the second-level domain name", e);
        throw new AnalyticsException("Unable to get the RDF document: " + args, e);
      }
    }
    doc.put(RDFDocument.DATASET_LABEL_FIELD, datasetLabel);
    return doc;
  }

  private String getContent(final TupleEntry arguments)
  throws AnalyticsException {
    final Object c = arguments.getObject(0);
    final String rdf;

    if (c instanceof String) {
      rdf = (String) c;
    } else if (c instanceof Text) {
      rdf = ((Text) c).toString();
    } else if (c instanceof Tuple) {
      rdf = ((Tuple) c).getString(0);
    } else {
      throw new AnalyticsException("Received empty RDF content");
    }
    return rdf;
  }

  private void parseNtriples(final TupleEntry arguments)
  throws AnalyticsException {
    final String rdf = getContent(arguments);
    final String[] triples;
    if (rdf == null || (triples = rdf.split("\n")).length == 0) {
      throw new AnalyticsException("Received empty triples");
    }

    final String defaultDomain;
    if (AnalyticsParameters.DEFAULT_DOMAIN.get() == null) {
      defaultDomain = "sindice.com";
    } else {
      defaultDomain = AnalyticsParameters.DEFAULT_DOMAIN.get();
    }

    final String d = "http://" + defaultDomain;
    doc.put(RDFDocument.DOMAIN_FIELD, defaultDomain);
    doc.put(RDFDocument.URL_FIELD, d);
    doc.put(RDFDocument.DATASET_URI_FIELD, d);
    doc.put(RDFDocument.EXPLICIT_CONTENT, triples);
    doc.put(RDFDocument.FORMAT, "NTRIPLE");
  }

  private void parseNQuads(final TupleEntry arguments)
  throws AnalyticsException {
    final String rdf = getContent(arguments);
    final String[] quads;
    if (rdf == null || (quads = rdf.split("\n")).length == 0) {
      throw new AnalyticsException("Received empty triples");
    }

    String context = null;
    String contextN3 = null;
    final List<String> triples = new ArrayList<String>();
    for (final String q : quads) {
      final String quad = q.trim().replaceFirst("\\s+\\.$", "");
      final int indContext = quad.lastIndexOf(' ');

      if (indContext != -1) {
        if (context == null) {
          try {
            final ValueFactory fact = new MemValueFactory();
            contextN3 = quad.substring(indContext + 1);
            context = NTriplesUtil.parseURI(contextN3, fact).stringValue();
          } catch(IllegalArgumentException e) {
            throw new AnalyticsException("Incorrect context URI: " + context, e);
          }
        } else if (!quad.endsWith(contextN3)) {
          throw new AnalyticsException("Received quads from more than one graph: " + rdf);
        }
        triples.add(quad.substring(0, indContext) + " .\n");
      } else {
        logger.error("Received incorrect quad: {}", quad);
      }
    }

    if (context != null) {
      doc.put(RDFDocument.URL_FIELD, context);
    } else {
      throw new AnalyticsException("Received incorrect quads: " + rdf);
    }
    doc.put(RDFDocument.EXPLICIT_CONTENT, triples.toArray(new String[triples.size()]));
    doc.put(RDFDocument.FORMAT, "QUADS");
  }

  private void parseJson(final TupleEntry arguments)
  throws AnalyticsException {
    // Gets the json line
    final String rdf = getContent(arguments);

    // Loads the json string
    final HashMap<String, Object> map;
    try {
      map = mapper.readValue(rdf, HashMap.class);
    } catch (Exception e) {
      logger.error("could not parse json " + StringUtils.abbreviate(rdf, 20), e);
      throw new AnalyticsException("Unable to get the RDF document: " + arguments);
    }
    Object value = map.get(RDFDocument.EXPLICIT_CONTENT);
    if (value == null) {
      map.put(RDFDocument.EXPLICIT_CONTENT, new ArrayList<String>());
    } else if (value instanceof String) {
      map.put(RDFDocument.EXPLICIT_CONTENT, Arrays.asList(value));
    }

    doc.putAll(map);
  }

}
