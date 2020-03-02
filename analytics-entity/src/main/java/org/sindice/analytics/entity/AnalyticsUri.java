/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.entity;

import java.nio.charset.Charset;

import org.openrdf.model.URI;
import org.openrdf.model.util.URIUtil;
import org.sindice.core.analytics.util.BytesRef;

/**
 * This {@link URI} implementation allows to set the label.
 */
public class AnalyticsUri
extends AnalyticsValue
implements URI {

  private static final long serialVersionUID = 4248303039949821370L;

  /**
   * An index indicating the first character of the local name in the URI
   * string, -1 if not yet set.
   */
  private int localNameIdx = -1;

  private String namespace;

  private String localname;

  /**
   * Create an {@link AnalyticsUri} instance with the an empty label.
   */
  public AnalyticsUri() {
    super();
  }

  /**
   * Create an {@link AnalyticsUri} instance with the given label (without the surrounding <code>&lt;...&gt;</code>).
   */
  public AnalyticsUri(String uri) {
    this(uri.getBytes(Charset.forName("UTF-8")));
  }

  /**
   * Create an {@link AnalyticsUri} instance with the given label (without the surrounding <code>&lt;...&gt;</code>).
   */
  public AnalyticsUri(byte[] uri) {
    super(uri);
  }

  /**
   * Create an {@link AnalyticsUri} instance with the given label (without the surrounding <code>&lt;...&gt;</code>).
   * The value starts at the position offset, for length bytes.
   */
  public AnalyticsUri(byte[] value, int offset, int length) {
    super(value, offset, length);
  }

  @Override
  public void reset() {
    super.reset();
    localNameIdx = -1;
    namespace = null;
    localname = null;
  }

  @Override
  public AnalyticsValue getCopy() {
    return new AnalyticsUri(value.bytes, value.offset, value.length);
  }

  @Override
  public String getNamespace() {
    if (namespace == null) {
      namespace = new String(value.bytes, 0, getLocalNameIdx(), Charset.forName("utf-8"));
    }
    return namespace;
  }

  @Override
  public String getLocalName() {
    if (localname == null) {
      localname = new String(value.bytes, getLocalNameIdx(), value.length - getLocalNameIdx(), Charset.forName("utf-8"));
    }
    return localname;
  }

  /**
   * Returns the index of the localname, as returned by {@link #getLocalNameIndex(byte[], int, int)}.
   */
  public int getLocalNameIdx() {
    if (localNameIdx == -1) {
      localNameIdx = getLocalNameIndex(value);
    }
    return localNameIdx;
  }

  @Override
  public String toString() {
    return stringValue();
  }

  @Override
  public byte getUniqueClassID() {
    return AnalyticsValue.URI_CODE;
  }

  /**
   * Finds the index of the first local name character in an (non-relative)
   * URI. This index is determined by the following the following steps:
   * <ul>
   * <li>Find the <em>first</em> occurrence of the '#' character,
   * <li>If this fails, find the <em>last</em> occurrence of the '/' character,
   * <li>If this fails, find the <em>last</em> occurrence of the ':' character.
   * <li>Add <tt>1<tt> to the found index and return this value.
   * </ul>
   * Note that the third step should never fail as every legal (non-relative)
   * URI contains at least one ':' character to seperate the scheme from the
   * rest of the URI. If this fails anyway, the method will throw an
   * {@link IllegalArgumentException}.
   * <p>
   * Copied from {@link URIUtil} for the Analytics use case.
   * 
   * @param uri a URI byte[] array in UTF-8 encoding.
   * @param offset the offset in the uri array
   * @param length the number of bits in the byte array starting at offset
   * @return The index of the first local name character in the URI string.
   *         Note that this index does not reference an actual character if the
   *         algorithm determines that there is not local name. In that case,
   *         the return index is equal to the length of the URI string.
   *         Returns 0 if the uri is <code>null</code>
   * @throws IllegalArgumentException
   *         If the supplied URI string doesn't contain any of the separator
   *         characters. Every legal (non-relative) URI contains at least one
   *         ':' character to seperate the scheme from the rest of the URI.
   */
  public static int getLocalNameIndex(byte[] uri, int offset, int length) {
    int separatorIdx = -1;

    // 1 byte because of UTF-8 encoding.

    // find first #
    for (int i = 0; i < uri.length; i++) {
      if (uri[i] == (byte) '#') {
        separatorIdx = i;
        break;
      }
    }
    if (separatorIdx < 0) {
      // find last /
      for (int i = uri.length - 1; i >= 0; i--) {
        if (uri[i] == (byte) '/') {
          separatorIdx = i;
          break;
        }
      }
    }
    if (separatorIdx < 0) {
      // find last :
      for (int i = uri.length - 1; i >= 0; i--) {
        if (uri[i] == (byte) ':') {
          separatorIdx = i;
          break;
        }
      }
    }

    if (separatorIdx < 0) {
      throw new IllegalArgumentException("No separator character founds in URI: "
        + new String(uri, offset, length, Charset.forName("UTF-8")));
    }
    return separatorIdx + 1;
  }

  /**
   * Finds the index of the first local name character in an (non-relative) URI.
   * @param uri the {@link BytesRef} holding the uri data
   * @see #getLocalNameIndex(byte[], int, int)
   */
  public static int getLocalNameIndex(BytesRef uri) {
    return getLocalNameIndex(uri.bytes, uri.offset, uri.length);
  }

}
