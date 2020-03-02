/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.rdf;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;

/**
 * This class provides an easy access over the Sesame {@link Statement} API.
 */
public class Triple implements Comparable<Triple> {

  private final Value subject;
  private final String subjectString;
  private final Value predicate;
  private final String predicateString;
  private final Value object;
  private final String objectString;

  /**
   * Create a {@link Triple} instance from the values.
   * @param statement the original statement
   * @param s the subject {@link Value}
   * @param p the predicate {@link Value}
   * @param o the object {@link Value}
   */
  public Triple(final Value s, final Value p, final Value o) {
    this.subject = s;
    this.subjectString = RDFParser.getStringValue(s);
    this.predicate = p;
    this.predicateString = RDFParser.getStringValue(p);
    this.object = o;
    this.objectString = RDFParser.getStringValue(o);
  }

  /**
   * @return the subject
   */
  public Value getSubject() {
    return subject;
  }

  /**
   * @return the subject as a {@link String}
   */
  public String getSubjectString() {
    return subjectString;
  }

  /**
   * @return the predicate
   */
  public Value getPredicate() {
    return predicate;
  }

  /**
   * @return the predicate as a {@link String}
   */
  public String getPredicateString() {
    return predicateString;
  }

  /**
   * @return the object
   */
  public Value getObject() {
    return object;
  }

  /**
   * @return the object as a {@link String}
   */
  public String getObjectString() {
    return objectString;
  }

  @Override
  public int compareTo(Triple triple) {
    final int s = subjectString.compareTo(triple.subjectString);
    if (s != 0) {
      return s;
    }
    final int p = predicateString.compareTo(triple.predicateString);
    if (p != 0) {
      return p;
    }
    return objectString.compareTo(triple.objectString);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Triple) {
      final Triple o = (Triple) obj;
      return subject.equals(o.subject) &&
             predicate.equals(o.predicate) &&
             object.equals(o.object);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hash = 1;
    hash = 31 * hash + subjectString.hashCode();
    hash = 31 * hash + predicateString.hashCode();
    hash = 31 * hash + objectString.hashCode();
    return hash;
  }

  @Override
  public String toString() {
    return "s=" + subject + " p=" + predicate + " o=" + object;
  }

}
