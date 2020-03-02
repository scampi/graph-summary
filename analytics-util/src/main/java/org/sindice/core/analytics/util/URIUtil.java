/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.util;

import java.net.URI;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.InternetDomainName;

/**
 * The {@link URIUtil} class provides methods for manipulating {@link URI}s.
 */
public class URIUtil {

  private static final Logger logger = LoggerFactory.getLogger(URIUtil.class);

  /**
   * Extract the domain name from the URL, then extract its second-level domain
   * name sing {@link URIUtil#getSndDomain(String)}.
   * @param url
   * @return
   */
  public static String getSndDomainFromUrl(String url) {
    if (url == null) {
      return null;
    }
    try {
      final String domain = URI.create(url).getHost();
      if (domain != null) {
        return getSndDomain(domain);
      }
    } catch (IllegalArgumentException e) {
      logger.error("", e);
    }
    logger.debug("The url={} isn't valid, skipping it", url);
    return null;
  }

  /**
   * Return the second-level domain name. Returns <code>null</code> if the
   * domain is not valid. This method normalises domain names by removing the
   * leading <code>www</code> sub-domain, if present.
   * @param domain
   * @return
   */
  public static String getSndDomain(String domain) {
    if (domain == null) {
      return null;
    }
    // Remove www subdomain if it exists
    if (domain.startsWith("www.")) {
      domain = domain.substring(4);
    }
    if (InternetDomainName.isValid(domain)) { // the domain is valid according to the RFC3490
      final InternetDomainName idn = InternetDomainName.from(domain);
      if (idn.hasPublicSuffix()) { // the domain has a public suffix
        if (idn.isUnderPublicSuffix()) {
          return idn.topPrivateDomain().name();
        } else if (idn.hasParent()) {
          final List<String> parts = idn.parts();
          return parts.get(parts.size() - 2).concat(".").concat(parts.get(parts.size() - 1));
        }
      }
    }
    return null;
  }

}
