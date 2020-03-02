/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.properties;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.sindice.core.analytics.util.AnalyticsUtil;
import org.sindice.graphsummary.cascading.SummaryParameters;
import org.sindice.graphsummary.cascading.properties.PropertiesCountSerialization;

/**
 * This {@link Map} keeps statistics related to predicates.
 * If {@link SummaryParameters#DATATYPE} is <code>true</code>, the distribution of datatypes per predicate
 * is also recorded.
 * @see PropertiesCountSerialization
 */
public class PropertiesCount1
extends TreeMap<Long, Map<Long, Long>>
implements Comparable<PropertiesCount1> {

  private static final long serialVersionUID = 5808409287987771866L;

  /**
   * The hash value for indicating the absence of datatype.
   */
  public static final long NO_DATATYPE = 0l;

  /**
   * Adds the predicate and the associated datatype to this {@link Map}.
   * If the key already exists, the value is the summed with the existing one.
   * @param label the predicate label to add
   * @param datatype the datatype associated with the predicate
   */
  public void add(long label, long datatype) {
    this.add(label, datatype, 1);
  }

  /**
   * Adds the predicate and the associated datatype to this {@link Map}.
   * If the key already exists, the value is the summed with the existing one.
   * @param label the predicate label to add
   * @param datatype the datatype associated with the predicate
   * @param count the number of occurrences
   */
  public void add(long label, long datatype, long count) {
    if (!containsKey(label)) {
      put(label, new TreeMap<Long, Long>());
    }
    final Map<Long, Long> map = get(label);
    if (!map.containsKey(datatype)) {
      map.put(datatype, 0l);
    }
    map.put(datatype, map.get(datatype) + count);
  }

  /**
   * Add all the statistics from the given {@link PropertiesCount1}.
   */
  public void add(PropertiesCount1 pc) {
    for (Entry<Long, Map<Long, Long>> entry : pc.entrySet()) {
      if (!containsKey(entry.getKey())) {
        put(entry.getKey(), entry.getValue());
      } else {
        final Map<Long, Long> map = get(entry.getKey());
        for (Entry<Long, Long> dt : entry.getValue().entrySet()) {
          if (map.containsKey(dt.getKey())) {
            map.put(dt.getKey(), map.get(dt.getKey()) + dt.getValue());
          } else {
            map.put(dt.getKey(), dt.getValue());
          }
        }
      }
    }
  }

  /**
   * Returns the total count associated with the given predicate label.
   * @param label the predicate hashed label
   * @return the total count
   */
  public long getCount(final long label) {
    long l = 0;
    final Map<Long, Long> datatypes = get(label);

    if (datatypes != null) {
      for (long count : datatypes.values()) {
        l += count;
      }
    }
    return l;
  }

  /**
   * Returns the count associated with the given predicate label and datatype.
   * @param label the predicate hashed label
   * @param datatype the hashed datatype
   * @return the count for this datatype
   */
  public long getCount(final long label, final long datatype) {
    long l = 0;
    final Map<Long, Long> datatypes = get(label);

    if (datatypes != null && datatypes.containsKey(datatype)) {
      l = datatypes.get(datatype);
    }
    return l;
  }

  @Override
  public int compareTo(PropertiesCount1 o) {
    final Iterator<Entry<Long, Map<Long, Long>>> thisSet = entrySet().iterator();
    final Iterator<Entry<Long, Map<Long, Long>>> otherSet = o.entrySet().iterator();

    while (thisSet.hasNext() && otherSet.hasNext()) {
      final Entry<Long, Map<Long, Long>> thisPair = thisSet.next();
      final Entry<Long, Map<Long, Long>> otherPair = otherSet.next();
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
    if (obj instanceof PropertiesCount1) {
      return compareTo((PropertiesCount1) obj) == 0;
    }
    return false;
  }

}
