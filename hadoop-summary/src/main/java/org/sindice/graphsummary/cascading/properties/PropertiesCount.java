/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.properties;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.BytesWritable;
import org.sindice.core.analytics.util.AnalyticsCounters;
import org.sindice.graphsummary.cascading.SummaryParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;

/**
 * This {@link Map} keeps statistics related to predicates.
 * If {@link SummaryParameters#DATATYPE} is <code>true</code>, the distribution of datatypes per predicate
 * is also recorded.
 * @see PropertiesCountSerialization
 */
public class PropertiesCount
implements Comparable<PropertiesCount> {

  private static final Logger logger      = LoggerFactory.getLogger(PropertiesCount.class);

  /**
   * The set of predicates, mapping to their set of datatypes cardinalities associated to a property.
   */
  final Map<Long, List<Long>>    ptrs        = new TreeMap<Long, List<Long>>();

  /**
   * The identifier of the cluster these properties are associated with.
   */
  private BytesWritable       clusterId   = null;

  /**
   * The hash value for indicating the absence of datatype.
   */
  public static final long    NO_DATATYPE = 0L;

  private FlowProcess         fp          = FlowProcess.NULL;

  /**
   * Iterates through the properties and datatypes.
   */
  public class Iterate {

    private final Iterator<Entry<Long, List<Long>>> entries;
    private long                                    property;
    private List<Long>                              datatypes;
    private int                                     pos = -2;

    public Iterate() {
      entries = ptrs.entrySet().iterator();
    }

    /**
     * Returns <code>true</code> if there is still a new property-datatype pair to be read.
     */
    public boolean getNext() {
      pos += 2;

      if (datatypes == null || pos == datatypes.size()) {
        if (entries.hasNext()) {
          final Entry<Long, List<Long>> e = entries.next();
          property = e.getKey();
          datatypes = e.getValue();
          pos = 0;
        } else {
          return false;
        }
      }
      return true;
    }

    /**
     * Returns the number of datatypes associated with this property.
     */
    public int getNbOfDatatypes() {
      return datatypes.size() / 2;
    }

    /**
     * Returns the property label.
     */
    public long getProperty() {
      return property;
    }

    /**
     * Returns the datatype label.
     */
    public long getDatatype() {
      return datatypes.get(pos);
    }

    /**
     * Returns the cardinality associated to this datatype and property.
     */
    public long getCount() {
      return datatypes.get(pos + 1);
    }

    @Override
    public String toString() {
      if (pos == -1) {
        return "";
      }
      return "property=" + property + " datatype=" + getDatatype() + " count=" + getCount();
    }

  }

  /**
   * The identifier of the cluster these properties are associated with.
   */
  public void setClusterId(BytesWritable clusterId) {
    this.clusterId = clusterId;
  }

  public void setFlowProcess(FlowProcess fp) {
    this.fp = fp;
  }

  /**
   * Returns the {@link Set} of property labels.
   */
  public Set<Long> keySet() {
    return ptrs.keySet();
  }

  /**
   * Returns an {@link Iterate} instance through which the properties, datatypes, and cardinalities can be accessed.
   */
  public Iterate iterate() {
    return new Iterate();
  }

  /**
   * Add the datatype with the given count to the list of datatypes.
   * @param dt a {@link List} of datatypes with the associated count
   * @param datatype the datatype label to add
   * @param count the count associated with the given datatype
   */
  private void addDatatypeCount(final List<Long> array, final long label, long count) {
    final int index = binarySearch(array, label, 0, array.size() / 2);

    array.set(index + 1, array.get(index + 1) + count);
  }

  /**
   * Binary search algorithm copied from {@link Arrays#binarySearch(long[], int, int, long)} in order to adapt to the
   * datatype array with double entries for the label and cardinality.
   * <p>
   * If the label is not found in the array, it is added at the insertion point and its cardinality is set to 0.
   * 
   * @param array the datatypes array
   * @param key the datatype label
   * @param imin the index of the first element (inclusive) to be searched
   * @param imax the index of the last element (exclusive) to be searched
   * @return the index of the datatype in the array
   */
  private int binarySearch(final List<Long> array, long key, int imin, int imax) {
    int low = imin;
    int high = imax - 1;

    while (low <= high) {
      final int mid = (low + high) >>> 1;
      final long midVal = array.get(mid * 2);

      if (midVal < key) {
        low = mid + 1;
      } else if (midVal > key) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }
    /*
     * key not found
     */
    final int insertIndex = low * 2;
    // allocate the space
    array.add(0L);
    array.add(0L);
    // shifts the data
    for (int i = array.size() - 4; i >= insertIndex; i -= 2) {
      array.set(i + 2, array.get(i));
      array.set(i + 3, array.get(i + 1));
    }
    // adds the key
    array.set(insertIndex, key);
    array.set(insertIndex + 1, 0L);
    return insertIndex;
  }

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
    final List<Long> counts = ptrs.get(label);

    if (counts == null) {
      final List<Long> dt = new ArrayList<Long>();
      dt.add(datatype);
      dt.add(count);
      ptrs.put(label, dt);
    } else {
      addDatatypeCount(counts, datatype, count);
    }
  }

  /**
   * Add all the statistics from the given {@link PropertiesCount}.
   */
  public void add(PropertiesCount o) {
    for (Entry<Long, List<Long>> oEntry : o.ptrs.entrySet()) {
      final long oLabel = oEntry.getKey();
      final List<Long> odt = oEntry.getValue();
      final List<Long> dt = ptrs.get(oLabel);

      if (dt == null) {
        ptrs.put(oLabel, odt);
      } else {
        for (int j = 0; j < odt.size(); j += 2) {
          final long datatype = odt.get(j);
          final long count = odt.get(j + 1);
          addDatatypeCount(dt, datatype, count);
        }
      }
    }
    if (size() >= 50000) {
      fp.increment(AnalyticsCounters.GROUP, "Cluster with many attributes", 1);
      logger.info("Cluster with many attributes: [{}] ({} attributes)", clusterId, size());
    }
  }

  /**
   * Returns the number of predicates.
   */
  public int size() {
    return ptrs.size();
  }

  /**
   * Clears the set of attributes.
   */
  public void clear() {
    ptrs.clear();
  }

  @Override
  public String toString() {
    return "ptrs=" + ptrs;
  }

  @Override
  public int compareTo(PropertiesCount o) {
    final Iterator<Entry<Long, List<Long>>> thisPtrs = ptrs.entrySet().iterator();
    final Iterator<Entry<Long, List<Long>>> otherPtrs = o.ptrs.entrySet().iterator();

    while (thisPtrs.hasNext() && otherPtrs.hasNext()) {
      final Entry<Long, List<Long>> thisPair = thisPtrs.next();
      final Entry<Long, List<Long>> otherPair = otherPtrs.next();

      if (thisPair.getKey().equals(otherPair.getKey())) { // for each property
        final List<Long> thisDt = thisPair.getValue();
        final List<Long> otherDt = otherPair.getValue();

        for (int i = 0; i < thisDt.size() && i < otherDt.size(); i++) { // for each datatype
          if (!thisDt.get(i).equals(otherDt.get(i))) {
            return thisDt.get(i).compareTo(otherDt.get(i));
          }
        }
        final int c = thisDt.size() - otherDt.size();
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
    if (obj instanceof PropertiesCount) {
      return compareTo((PropertiesCount) obj) == 0;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return 31 + ptrs.hashCode();
  }

}
