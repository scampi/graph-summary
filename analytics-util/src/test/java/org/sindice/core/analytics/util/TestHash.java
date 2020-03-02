/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.util;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * 
 */
public class TestHash {

  @Test
  public void test() {
    assertTrue(Hash.getHash32("test1") != Hash.getHash32("test2"));
    assertTrue(Hash.getHash32("test1", "test2") != Hash.getHash32("test2"));
    assertTrue(Hash.getHash32("test1", "test2") != Hash.getHash32("test2", "test1"));
    assertTrue(Hash.getHash32("test1test3") == Hash.getHash32("test1", "test3"));
  }

  @Test
  public void testLong() {
    assertTrue(Hash.getHash64("test1") != Hash.getHash64("test2"));
    assertTrue(Hash.getHash64("test1", "test2") != Hash.getHash64("test2"));
    assertTrue(Hash.getHash64("test1", "test2") != Hash.getHash64("test2", "test1"));
    assertTrue(Hash.getHash64("test1", "test3") == Hash.getHash64("test1", "test3"));
  }

}
