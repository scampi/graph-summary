/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.rdf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.openrdf.model.Statement;
import org.sindice.core.analytics.util.AnalyticsException;
import org.sindice.core.analytics.util.URIUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a document with RDF content.
 * 
 * <p>
 * 
 * The document is modelled as a set of fields, each field contains
 * a field name with an associated {@link String[]} value.
 * The fields {@value #EXPLICIT_CONTENT} and {@value #IMPLICIT_CONTENT}
 * contain the RDF data. other field can be used to store metadata such
 * as the field {@value #FORMAT} that describes the format
 * of the RDF data.
 */
public class RDFDocument {

  private static final Logger         logger              = LoggerFactory.getLogger(RDFDocument.class);

  /** Sindice fields: RDF data */
  public static final String          EXPLICIT_CONTENT    = "explicit_content";
  /** Sindice fields: RDF data inferred from {@value #EXPLICIT_CONTENT} */
  public static final String          IMPLICIT_CONTENT    = "implicit_content";
  /** Sindice fields: the formats of the RDF data in {@value #EXPLICIT_CONTENT} */
  public static final String          FORMAT              = "format";

  /**
   * Extract the dataset label, i.e., the second-level domain, from these fields
   */
  public static final String          DOMAIN_FIELD        = "domain";
  public static final String          DATASET_URI_FIELD   = "dataset_uri";
  public static final String          URL_FIELD           = "url";
  /** To store the dataset label */
  public static final String          DATASET_LABEL_FIELD = "dataset-label";

  private final Map<String, String[]> fields              = new HashMap<String, String[]>();

  private final RDFParser parser;

  RDFDocument(RDFParser parser) {
    this.parser = parser;
  }

  /**
   * Clear the data in this document.
   */
  public void clear() {
    fields.clear();
  }

  /**
   * Return the value of the given field. </br> Return null if field is empty,
   * and {@code List<String>} of the field is multi-valued.
   */
  public List<String> get(final String field) {
    final String[] val = fields.get(field);
    if (val == null || val.length == 0) {
      return null;
    }
    return Arrays.asList(val);
  }

  /**
   * Returns a list of {@link Triple}s, which statements come from the
   * {@link #CONTENT_FIELDS} of this {@link RDFDocument}.
   * @deprecated
   */
  public List<Triple> getTriples() {
    final List<Triple> triples = new ArrayList<Triple>();

    final String[] val = fields.get(EXPLICIT_CONTENT);
    for (String statement : val) {
      final Statement st = parser.parseStatement(statement);
      if (st == null) {
        logger.error("Error parsing the triple for the string {}, " +
            "skipping: ", statement);
        continue;
      }
      triples.add(new Triple(st.getSubject(), st.getPredicate(), st.getObject()));
    }
    return triples;
  }

  /**
   * Returns the array of triples, unparsed.
   */
  public String[] getContent() {
    final String[] val = fields.get(EXPLICIT_CONTENT);
    return val;
  }

  /**
   * Return the url of the document.
   * @throws AnalyticsException if the url is null or empty
   */
  public String getUrl()
  throws AnalyticsException {
    final String url = fields.get(URL_FIELD)[0];
    if (url == null || url.isEmpty()) {
      logger.error("URL is invalid: {}", Arrays.toString(fields.get(URL_FIELD)));
      throw new AnalyticsException();
    }
    return url;
  }

  /**
   * Return the dataset label of the document.
   * @throws AnalyticsException if the dataset label is null or empty
   */
  public String getDatasetLabel()
  throws AnalyticsException {
    if (!fields.containsKey(DATASET_LABEL_FIELD)) {
      logger.error("Missing field: {}", DATASET_LABEL_FIELD);
      throw new AnalyticsException();
    }
    final String ds = fields.get(DATASET_LABEL_FIELD)[0];
    if (ds == null || ds.isEmpty()) {
      logger.error("Dataset label is invalid: {}", Arrays.toString(fields.get(DATASET_LABEL_FIELD)));
      throw new AnalyticsException();
    }
    return ds;
  }

  /**
   * Set the value of the given field. </br> The expected values are:
   * <ul>
   * <li> {@code String} if the field is single-valued,
   * <li> {@code List<String>} of the field is multi-valued.
   * <ul>
   */
  public void put(final String field, final Object object) {
    if (object instanceof Collection<?>) {
      try {
        fields.put(field, ((Collection<?>) object).toArray(new String[0]));
      } catch (final ArrayStoreException e) { // SND-885
        final Object[] array = ((Collection<?>) object).toArray(new Object[0]);
        final String[] values = new String[array.length];
        for (int i = 0; i < values.length; i++) {
          values[i] = array[i] == null ? null : array[i].toString();
        }
        fields.put(field, values);
      }
    } else {
      if (object != null) {
        if (object instanceof String[]) {
          fields.put(field, (String[]) object);
        } else {
          fields.put(field, new String[] { object.toString() });
        }
      }
    }
  }

  /**
   * Copy the content of the map inside the HbaseDocument object. Each key-value
   * pair will become a field-value.
   */
  public void putAll(final Map<String, Object> map) {
    for (Entry<String, Object> s : map.entrySet()) {
      this.put(s.getKey(), s.getValue());
    }
  }

  /**
   * Return the values of a given field into an array of String:
   * <ul>
   * <li>the array is empty if the field does no exist;
   * <li>the array contains one element if the field is single-valued;
   * <li>the array contains multiple elements if the field is multi-valued.
   * <ul>
   */
  public String[] getRaw(final String field) {
    String[] values = null;
    if (fields.containsKey(field)) {
      values = fields.get(field);
      if (values == null) {
        return new String[] {};
      }
      return values;
    }
    return new String[] {};
  }

  public void putRaw(final String field, final String[] value) {
    fields.put(field, value);
  }

  /**
   * Returns the domain name of the document where the RDF
   * data comes from.
   * 
   * <p>
   * 
   * This method tries to extract the domain name from the fields:
   * <ul>
   * <li>{@value #DOMAIN_FIELD};</li>
   * <li>{@value #DATASET_URI_FIELD}; and</li>
   * <li>{@value #URL_FIELD}.</li>
   * </ul>
   * 
   * @throws AnalyticsException if no domain name could be extracted.
   */
  public String getDomain()
  throws AnalyticsException {
    final String[] domains;
    if (this.getRaw(RDFDocument.DOMAIN_FIELD).length != 0) {
      domains = this.getRaw(RDFDocument.DOMAIN_FIELD);
      String domain = "";
      for (String d : domains) {
        if (d.length() > domain.length()) {
          domain = d;
        }
      }
      return domain.toLowerCase();
    } else if (this.getRaw(RDFDocument.DATASET_URI_FIELD).length != 0) {
      domains = this.getRaw(RDFDocument.DATASET_URI_FIELD);
    } else if (this.getRaw(RDFDocument.URL_FIELD).length != 0) {
      domains = this.getRaw(RDFDocument.URL_FIELD);
    } else {
      logger.error("No Dataset label could be extracted: No field ["
          + RDFDocument.DOMAIN_FIELD + "," + RDFDocument.DATASET_URI_FIELD
          + "," + RDFDocument.URL_FIELD + "]");
      throw new AnalyticsException();
    }

    /*
     * if I'm parsing the domain, and there is more then one domain, i'll take
     * the longer.
     */
    String domain = null;
    for (String d : domains) {
      try {
        final String d2 = java.net.URI.create(d).getHost();
        if (domain == null || (d2 != null && d2.length() > domain.length())) {
          if (d2 == null) {
            continue;
          }
          // Check that the second-level domain name can be extracted
          // from that uri
          final String snd = URIUtil.getSndDomain(d2);
          if (snd != null) {
            domain = d2;
          }
        }
      } catch (IllegalArgumentException e) {
        logger.debug("domain={} is invalid: {}", domain, e);
        domain = null;
      }
    }
    if (domain == null) {
      throw new AnalyticsException("Unable to extract the domain for the " +
          "document: " + this);
    }
    return domain.toLowerCase();
  }

  /**
   * Return the second-level domain name of the document where the RDF data
   * comes from.
   * @throws AnalyticsException if the second-level domain name couldn't be extracted
   */
  public String getSndDomain()
  throws AnalyticsException {
    final String domain = this.getDomain();
    final String sndDomain = URIUtil.getSndDomain(domain);

    if (sndDomain == null) {
      logger.error("Invalid second-domain extracted from domain '{}'", domain);
      throw new AnalyticsException();
    }
    return sndDomain;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    for (Entry<String, String[]> field: fields.entrySet()) {
      sb.append(field.getKey()).append(": ")
        .append(Arrays.toString(field.getValue())).append('\n');
    }
    return sb.toString();
  }

}
