/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.util;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * 
 */
public class TestMurmurHash3 {

  @Test
  public void test1() {
    final byte[] b1 = { 1, 1, 1, 1 };
    final byte[] b2 = { 2, 2, 2, 1, 1, 1, 1, 3 };
    assertEquals(Hash.getHash128(b1), Hash.getHash128(b2, 3, 4));
  }

  @Test
  public void test2() {
    final byte[] b1 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                        17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33 };
    final byte[] b2 = { 2, 2, 2, 1, 1, 1, 1, 3,
                        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                        17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33,
                        2, 2, 2, 1, 1, 1, 1, 3 };
    assertEquals(Hash.getHash128(b1), Hash.getHash128(b2, 8, 33));
  }

}
