/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.properties;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.sindice.core.analytics.rdf.AnalyticsClassAttributes;
import org.sindice.core.analytics.util.AnalyticsUtil;
import org.sindice.core.analytics.util.Hash;

/**
 * This {@link Map} keeps statistics about the usage of types.
 * The key is the {@link Hash#getHash64(String) hashed} label of a type, and the value is a {@link Map}
 * giving counts divided per {@link AnalyticsClassAttributes class attributes}. Each class attribute is identified by
 * its index in the {@link AnalyticsClassAttributes#CLASS_ATTRIBUTES} list.
 * @see TypesCountSerialization
 */
public class TypesCount
extends TreeMap<Long, Map<Byte, Long>>
implements Comparable<TypesCount> {

  private static final long serialVersionUID = 5808409287987771866L;

  /**
   * Adds the type defined with the given predicate.
   * @param type the type label as a {@link Hash#getHash64(byte[])}
   * @param predicate the predicate label as a {@link Hash#getHash64(byte[])}
   */
  public void add(long type, byte predicate) {
    add(type, predicate, 1l);
  }

  /**
   * Adds the type defined with the given predicate, with the pair occurring nTimes.
   * @param type the type label as a {@link Hash#getHash64(byte[])}
   * @param predicate the predicate label as a {@link Hash#getHash64(byte[])}
   * @param nTimes the number of label-predicate pairs
   */
  public void add(long type, byte predicate, long nTimes) {
    if (!containsKey(type)) {
      put(type, new TreeMap<Byte, Long>());
    }
    final Map<Byte, Long> map = get(type);
    final byte attrIndex = predicate;
    if (!map.containsKey(attrIndex)) {
      map.put(attrIndex, 0L);
    }
    map.put(attrIndex, map.get(attrIndex) + nTimes);
  }

  /**
   * Add all the statistics from the given {@link TypesCount}
   */
  public void add(TypesCount pc) {
    for (Entry<Long, Map<Byte, Long>> entry : pc.entrySet()) {
      if (!containsKey(entry.getKey())) {
        put(entry.getKey(), entry.getValue());
      } else {
        final Map<Byte, Long> map = get(entry.getKey());
        for (Entry<Byte, Long> dt : entry.getValue().entrySet()) {
          if (map.containsKey(dt.getKey())) {
            map.put(dt.getKey(), map.get(dt.getKey()) + dt.getValue());
          } else {
            map.put(dt.getKey(), dt.getValue());
          }
        }
      }
    }
  }

  @Override
  public int compareTo(TypesCount o) {
    final Iterator<Entry<Long, Map<Byte, Long>>> thisSet = entrySet().iterator();
    final Iterator<Entry<Long, Map<Byte, Long>>> otherSet = o.entrySet().iterator();

    while (thisSet.hasNext() && otherSet.hasNext()) {
      final Entry<Long, Map<Byte, Long>> thisPair = thisSet.next();
      final Entry<Long, Map<Byte, Long>> otherPair = otherSet.next();
      if (thisPair.getKey().equals(otherPair.getKey())) {
        final int c = AnalyticsUtil.compareMaps(thisPair.getValue(), otherPair.getValue());
        if (c != 0) {
          return c;
        }
      } else {
        return thisPair.getKey().compareTo(otherPair.getKey());
      }
    }
    return size() - o.size();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TypesCount) {
      return compareTo((TypesCount) obj) == 0;
    }
    return false;
  }

}
