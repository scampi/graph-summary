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

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author diego
 */
public class Predicate2Namespace {

  private static final Logger logger = LoggerFactory.getLogger(Predicate2Namespace.class);

  private AnalyticsProperties properties;

  public Predicate2Namespace() {
    this("/rules.properties");
  }

  public Predicate2Namespace(String propertiesFile) {
    properties = new AnalyticsProperties();
    InputStream in = getClass().getResourceAsStream(propertiesFile);
    try {
      properties.load(in);
    } catch (IOException e) {
      logger.error("Cannot load the rules for converting the predicates in namespaces ({})", e.toString());
      return;
    }
  }

  public boolean isRemove(String namespace) {
    return properties.isRemove(namespace);
  }

  public Set<String> getFilters() {
    return properties.getPatternToDelete();
  }

  public Map<String, String> getNamespace2Uri() {
    return properties.getNamespace2Uri();
  }

  public String getNamespace(String predicate) {

    String namespace = "";
    predicate = properties.replace(predicate);
    namespace = properties.getReplacement(predicate);

    if (namespace == null)
      if (properties.isNormalizationEnabled()) {
        int pos = predicate.length() - 1;
        while ((pos >= 0) && (predicate.charAt(pos) != '#') &&
               (predicate.charAt(pos) != '/') && (predicate.charAt(pos) != ':'))
          pos--;
        while ((pos >= 0) &&
               ((predicate.charAt(pos) == '#') ||
                (predicate.charAt(pos) == '/') || (predicate.charAt(pos) == ':')))
          pos--;
        if (pos > 0)
          namespace = predicate.substring(0, pos + 1);
        else
          namespace = predicate;
        try {
          namespace = UriNormalisationUtil.normalize(namespace);
        } catch (URISyntaxException e) {
          logger.error("Predicate {} is not a valid URI", namespace);
          return null;
        }
      } else
        namespace = predicate;

    namespace = UriNormalisationUtil.normalizePrefix(namespace);
    return namespace;
  }

}
