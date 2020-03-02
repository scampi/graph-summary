/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.util;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

/**
 * Implementation of the {@link StringReader} that allows to reset the reader
 * to a new string input.
 */
public final class ReusableStringReader extends Reader {

  private int    pos  = 0;
  private int    size = 0;
  private String s    = null;

  public void setValue(final String s) {
    this.s = s;
    this.size = s.length();
    this.pos = 0;
  }

  @Override
  public int read() {
    if (pos < size) {
      return s.charAt(pos++);
    } else {
      s = null;
      return -1;
    }
  }

  @Override
  public int read(final char[] c, final int off, int len) {
    if (pos < size) {
      len = Math.min(len, size-pos);
      s.getChars(pos, pos+len, c, off);
      pos += len;
      return len;
    } else {
      s = null;
      return -1;
    }
  }

  @Override
  public void close()
  throws IOException {
    pos = size; // this prevents NPE when reading after close!
    s = null;
  }

}
