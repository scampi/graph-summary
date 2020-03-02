/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.rdf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.sindice.analytics.entity.AnalyticsUri;
import org.sindice.analytics.entity.AnalyticsValue;

/**
 * This class contains the definition of class attributes.
 * 
 * <p>
 * 
 * Class attributes are predicates which define the associated object
 * as a class. The default predicate used as a class attribute is
 * <code>rdf:type</code>.
 */
public class AnalyticsClassAttributes {

  /** The default class attribute, i.e., <code>rdf:type</code> */
  public static final String       DEFAULT_CLASS_ATTRIBUTE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

  /**
   * The list of class attributes. There cannot be more than 256 class attributes.
   */
  public static List<AnalyticsUri> CLASS_ATTRIBUTES;

  public static final void initClassAttributes(String[] attributes) {
    initClassAttributes(Arrays.asList(attributes));
  }

  public static final void initClassAttributes(List<String> attributes) {
    Collections.sort(attributes);
    final List<AnalyticsUri> caBytes = new ArrayList<AnalyticsUri>();
    for (String ca : attributes) {
      caBytes.add(new AnalyticsUri(ca));
    }
    CLASS_ATTRIBUTES = Collections.unmodifiableList(caBytes);
  }

  /**
   * Normalises the class literal, which is in the NTriple format.
   * 
   * <p>
   * 
   * The normalisation of the class label consists in the following operations:
   * <ul>
   * <li>remove trailing whitespace;</li>
   * <li>lowercase the label; and</li>
   * <li>replace multiple connected whitespace characters into one.</li>
   * </ul>
   * 
   * <p>
   * 
   * After normalisation, the double quotes are wrapped around the label.
   * <p>
   * <b>GL-64</b> issue highlighted the need of wrapping the label with double quotes.
   * This is to distinguish between {@link Literal} and {@link URI} type labels, when only the {@link String} value
   * is available.
   * 
   * @param input the class to normalize
   * @return <code>null</code> if the normalisation of the label produced an empty string
   * @deprecated use {@link #normalizeLiteralTypeLabel(String)}
   */
  public static String normalizeLiteralType(String input) {
    // normalise the literals of type
    int startIndex = input.indexOf("\"");
    int endIndex = input.lastIndexOf("\"");

    if (endIndex - startIndex > 0) {
      // Remove language identifier
      input = input.substring(startIndex + 1, endIndex);
      // Remove extra whitespace and lowercase
      input = input.trim().toLowerCase();
      // Replace any whitespace with a single space
      input = input.replaceAll("\\s+", " ");
      // add quotation marks back on
      if (!input.isEmpty()) {
        return "\"" + input + "\"";
      }
    }
    return null;
  }

  /**
   * Normalises the label of the class literal
   * 
   * <p>
   * 
   * The normalisation of the class label consists in the following operations:
   * <ul>
   * <li>remove trailing whitespace;</li>
   * <li>lowercase the label; and</li>
   * <li>replace multiple connected whitespace characters into one.</li>
   * </ul>
   * 
   * <p>
   * 
   * After normalisation, the double quotes are wrapped around the label.
   * <p>
   * <b>GL-64</b> issue highlighted the need of wrapping the label with double quotes.
   * This is to distinguish between {@link Literal} and {@link URI} type labels, when only the {@link String} value
   * is available.
   * With <b>GL-103</b> there is no more need to add the quotes.
   * 
   * @param label the label of the class
   * @return <code>null</code> if the normalisation of the label produced an empty string
   */
  public static String normalizeLiteralTypeLabel(String label) {
    final StringBuilder sb = new StringBuilder();
    int start = 0;
    int end = 0;

    if (label == null || label.isEmpty()) {
      return null;
    }
    // Lower case
    label = label.toLowerCase();
    // Remove trailing whitespace
    for (int i = 0; i < label.length(); i++) {
      if (!isWhitespace(label.charAt(i))) {
        start = i;
        break;
      }
    }
    for (int i = label.length() - 1; i >= 0; i--) {
      if (!isWhitespace(label.charAt(i))) {
        end = i;
        break;
      }
    }

    // Squeeze blank spaces
    int nWS = 0;
    sb.setLength(0);
    for (int i = start; i <= end; i++) {
      if (isWhitespace(label.charAt(i))) {
        nWS++;
      } else {
        nWS = 0;
      }
      if (nWS <= 1) {
        sb.append(label.charAt(i));
      }
    }
    if (sb.length() != 0) {
      return sb.toString();
    }
    return null;
  }

  /**
   * Returns <code>true</code> if the character is a whitespace.
   */
  private static boolean isWhitespace(char c) {
    if (c == ' ' || c == '\t' || c == '\n' || c == '\u000B' ||
        c == '\f' || c == '\r') {
      return true;
    }
    return false;
  }

  /**
   * Return true if the predicate is a class attribute.
   * @deprecated use {@link #isClass(AnalyticsValue)}
   */
  public static boolean isClass(String predicate) {
    return CLASS_ATTRIBUTES.contains(new AnalyticsUri(predicate));
  }

  /**
   * Return true if the predicate is a class attribute.
   */
  public static boolean isClass(AnalyticsValue predicate) {
    return CLASS_ATTRIBUTES.contains(predicate);
  }

}
