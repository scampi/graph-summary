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
package org.sindice.core.analytics.stats.assembly;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Read a .properties file containing the rules to manage the predicates for the
 * stats.
 * 
 */
public class AnalyticsProperties {
  public enum PatternType {
    DELETE, REPLACE, REPLACE_PREFIX, REPLACE_SUFFIX, REPLACE_ALL
  }

  private boolean normalizationEnabled = false;
  private boolean includeClasses = false;

  // one map for each different type of rule:
  // delete: deletes all the predicate matching the pattern, e.g.,
  // anystring = -
  // will delete www.anystringasd.org/foaf, www.sindice.org/anystring, ...
  private Set<Rule> delete;
  // replace: replaces a pattern in the predicate with another pattern, e.g.,
  // purl.org/dc/elements/1.0 = purl.org/dc/elements/1.1
  // will transform:
  // http://purl.org/dc/elements/1.0/#something in
  // http://purl.org/dc/elements/1.1/#something
  private Set<Rule> replace;
  // replacePrefix: replace a pattern in the predicate, removing the prefix,
  // e.g.,
  // *facebook.com* = www.facebook.com
  // this will produce, for all predicates containing the pattern
  // facebook.com, the namespace 'www.facebook.com'
  private Set<Rule> replacePrefix;
  // replaceSuffix: replaces a pattern in the predicate, removing the suffix,
  // e.g.,
  // purl.org/dc/elements* = purl.org/dc/elements
  // will transform: http://purl.org/dc/elements/1.0/#something in
  // http://purl.org/dc/elements
  // and http://purl.org/dc/elements/1.0/#something in
  // http://purl.org/dc/elements
  // and so on..
  private Set<Rule> replaceSuffix;
  // replaceAll: replacePrefix+replaceSuffix
  private Set<Rule> replaceAll;

  // original values in order to output the original url (with http, https www)
  private Map<String, String> namespace2originalURI;

  public AnalyticsProperties() {
    delete = new HashSet<Rule>();
    replace = new HashSet<Rule>();
    replacePrefix = new HashSet<Rule>();
    replaceSuffix = new HashSet<Rule>();
    replaceAll = new HashSet<Rule>();
    namespace2originalURI = new HashMap<String, String>();
  }

  public void load(InputStream inStream) throws IOException {
    BufferedReader file = new BufferedReader(new InputStreamReader(inStream));
    String line;
    while ((line = file.readLine()) != null) {
      if (isComment(line))
        continue;
      Rule r = new Rule(line);
      if (r.getPattern().equals("ENABLE_AUTOMATIC_NORMALIZZATION")) {
        if (r.getValue().equals("true"))
          normalizationEnabled = true;
        continue;
      }
      if (r.getPattern().equals("INCLUDE_CLASSES")) {
        if (r.getValue().equals("true"))
          includeClasses = true;
        continue;
      }
      switch (r.type) {
      case DELETE:
        delete.add(r);
        break;
      case REPLACE:
        replace.add(r);
        break;
      case REPLACE_PREFIX:
        replacePrefix.add(r);
        break;
      case REPLACE_SUFFIX:
        replaceSuffix.add(r);
        break;
      case REPLACE_ALL:
        replaceAll.add(r);
        break;

      }

    }
  }

  /*
   * only replace patterns that not containing stars
   */
  public String replace(String line) {

    for (Rule r : replace) {
      if (r.match(line))
        return r.apply(line);
    }
    return line;
  }

  /*
   * given a string returns the replacement
   */
  public String getReplacement(String line) {
    ArrayList<Set<Rule>> ruleSets = new ArrayList<Set<Rule>>();
    // ruleSets.add(replace);
    ruleSets.add(replaceAll);
    ruleSets.add(replacePrefix);
    ruleSets.add(replaceSuffix);
    for (Set<Rule> rules : ruleSets) {
      for (Rule r : rules) {
        if (r.match(line))
          return r.apply(line);
      }
    }
    return null;
  }

  /**
   * @return the enableNormalization
   */
  public boolean isNormalizationEnabled() {
    return normalizationEnabled;
  }

  /**
   * @param enableNormalization
   *          the enableNormalization to set
   */
  public void setNormalizationEnabled(boolean enableNormalization) {
    this.normalizationEnabled = enableNormalization;
  }

  public boolean isRemove(String line) {
    for (Rule r : delete) {
      if (r.match(line))
        return true;
    }
    return false;
  }

  public String originalURI(String namespace) {
    return namespace2originalURI.get(namespace);
  }

  private boolean isComment(String line) {
    int i = 0;
    // scan the line until it reads something that is not a space or it
    // reach the end of the line
    while ((i < line.length()) && (line.charAt(i) == ' '))
      i++;
    // empty line
    if (i == line.length())
      return true;
    if ((line.charAt(i) == '!') || (line.charAt(i) == '#'))
      return true;
    return false;
  }

  public Set<String> getPatternToDelete() {
    Set<String> deletePattern = new HashSet<String>();
    for (Rule r : delete) {
      deletePattern.add(r.getPattern());
    }
    return deletePattern;
  }

  private class Rule {

    private PatternType type;
    private String pattern;
    private String value;

    public Rule(String line) throws IOException {
      String[] elems = line.split("=");
      if (elems.length != 2) {
        throw new IOException("Error parsing the line " + line);
      }
      pattern = UriNormalisationUtil.normalizePrefix(elems[0].trim());
      value = UriNormalisationUtil.normalizePrefix(elems[1].trim());
      namespace2originalURI.put(value, elems[1].trim());
      type = parseType(pattern, value);
      pattern = pattern.replaceAll("[*]", "");
    }

    private PatternType parseType(String _pattern, String _value) {
      if (_value.equals("-"))
        return PatternType.DELETE;
      if ((isReplacePrefix(_pattern)) && (isReplaceSuffix(_pattern)))
        return PatternType.REPLACE_ALL;
      if (isReplacePrefix(_pattern))
        return PatternType.REPLACE_PREFIX;
      if (isReplaceSuffix(_pattern))
        return PatternType.REPLACE_SUFFIX;
      return PatternType.REPLACE;
    }

    private boolean isReplacePrefix(String _pattern) {
      return (_pattern.charAt(0) == '*');
    }

    private boolean isReplaceSuffix(String _pattern) {
      return (_pattern.charAt(_pattern.length() - 1) == '*');
    }

    public String getPattern() {
      return pattern;
    }

    public String getValue() {
      return value;
    }

    public boolean match(String line) {
      return line.contains(pattern);
    }

    public String apply(String line) {
      if (type == PatternType.DELETE)
        return null;
      if (type == PatternType.REPLACE_ALL)
        return value;
      if (type == PatternType.REPLACE_PREFIX) {
        int pos = line.indexOf(pattern);
        line = line.substring(pos);
        line = line.replace(pattern, value);
        return line;
      }
      if (type == PatternType.REPLACE_SUFFIX) {
        int pos = line.indexOf(pattern);
        line = line.substring(0, pos);
        line = line + value;
        return line;
      }
      if (type == PatternType.REPLACE) {
        line = line.replace(pattern, value);
        return line;
      }
      return null;
    }

    @Override
    public String toString() {
      if (type == PatternType.DELETE) {
        return "DELETE: " + pattern;
      } else
        return pattern + " --> " + value;
    }

  }

  /**
   * @return the includeClasses
   */
  public boolean isIncludeClasses() {
    return includeClasses;
  }

  /**
   * @param includeClasses
   *          the includeClasses to set
   */
  public void setIncludeClasses(boolean includeClasses) {
    this.includeClasses = includeClasses;
  }

  /**
   * @return
   */
  public Map<String, String> getNamespace2Uri() {
    return namespace2originalURI;
  }

}
