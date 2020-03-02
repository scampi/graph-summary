/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.entity;

import static org.sindice.core.analytics.util.AnalyticsCounters.GROUP;
import static org.sindice.core.analytics.util.AnalyticsCounters.TIME;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.openrdf.model.Value;
import org.sindice.core.analytics.util.Hash;

/**
 * This implementation of {@link EntityDescription} stores the entity description as a list of predicate-objects,
 * where the objects are the {@link Value object} associated with this predicate.
 */
public class MapEntityDescription
extends AbstractEntityDescription {

  /** The set of predicate-objects about this entity */
  final Map<AnalyticsValue, Set<AnalyticsValue>> sts = new HashMap<AnalyticsValue, Set<AnalyticsValue>>();

  /** The identifier of this entity */
  AnalyticsValue                                 subject;

  /** The number of statements about this entity */
  int                                            size;

  @Override
  public Statements iterateStatements() {
    fp.increment(GROUP, PO_COUNTER + this.getClass().getSimpleName(), getNbStatements());
    logLargeEntity(getNbStatements());
    return new MapEntityStatements();
  }

  private class MapEntityStatements implements Statements {

    private final Iterator<Entry<AnalyticsValue, Set<AnalyticsValue>>> it;
    private Iterator<AnalyticsValue> objIt;

    private AnalyticsValue predicate;
    private AnalyticsValue object;

    public MapEntityStatements() {
      it = sts.entrySet().iterator();
    }

    @Override
    public boolean getNext() {
      final long start = System.currentTimeMillis();

      if (objIt == null || !objIt.hasNext()) {
        if (!it.hasNext()) {
          fp.increment(GROUP, TIME + this.getClass().getSimpleName() + "#getNext", System.currentTimeMillis() - start);
          return false;
        }
        final Entry<AnalyticsValue, Set<AnalyticsValue>> pos = it.next();
        predicate = pos.getKey();
        objIt = pos.getValue().iterator();
        return getNext();
      }
      object = objIt.next();
      fp.increment(GROUP, TIME + this.getClass().getSimpleName() + "#getNext", System.currentTimeMillis() - start);
      return true;
    }

    @Override
    public AnalyticsValue getPredicate() {
      return predicate;
    }

    @Override
    public AnalyticsValue getPredicateCopy() {
      return predicate.getCopy();
    }

    @Override
    public long getPredicateHash64() {
      return Hash.getHash64(predicate.getValue());
    }

    @Override
    public AnalyticsValue getObject() {
      return object;
    }

    @Override
    public AnalyticsValue getObjectCopy() {
      return object.getCopy();
    }

    @Override
    public long getObjectHash64() {
      return Hash.getHash64(object.getValue());
    }

    @Override
    public BytesWritable getObjectHash128() {
      return BlankNode.getHash128(object);
    }

    @Override
    public String toString() {
      return "p=" + getPredicate() + " o=" + getObject();
    }

  }

  @Override
  public void reset() {
    sts.clear();
    size = 0;
    subject = null;
  }

  @Override
  public void add(AnalyticsValue s, AnalyticsValue p, AnalyticsValue o) {
    if (subject == null) {
      subject = s.getCopy();
    }
    if (!sts.containsKey(p)) {
      sts.put(p.getCopy(), new HashSet<AnalyticsValue>());
    }
    if (!sts.get(p).contains(o)) {
      sts.get(p).add(o.getCopy());
    }
    size++;
  }

  @Override
  public void add(EntityDescription entity) {
    if (subject == null) {
      subject = entity.getEntity();
    }
    if (entity instanceof MapEntityDescription) {
      // add the statements
      for (Entry<AnalyticsValue, Set<AnalyticsValue>> pos : ((MapEntityDescription) entity).sts.entrySet()) {
        final AnalyticsValue predicate = pos.getKey();
        final Set<AnalyticsValue> objects = pos.getValue();

        if (sts.containsKey(predicate)) { // existing predicate
          final Set<AnalyticsValue> thisObjects = sts.get(predicate);
          for (AnalyticsValue o : objects) {
            if (thisObjects.add(o)) {
              size++;
            }
          }
        } else { // new predicate
          sts.put(predicate, objects);
          size += objects.size();
        }
      }
    } else {
      throw new ClassCastException("Cannot add the statements from entity=" + entity.getClass().getName() + " to "
        + getClass().getName());
    }
  }

  @Override
  public AnalyticsValue getEntity() {
    return subject;
  }

  @Override
  public int getNbStatements() {
    return size;
  }

  @Override
  public int compareTo(EntityDescription o) {
    if (o instanceof MapEntityDescription) {
      final MapEntityDescription other = (MapEntityDescription) o;
      if (subject == null && other.subject == null) {
        return 0;
      }
      if (subject == null) {
        return 1;
      }
      if (other.subject == null) {
        return -1;
      }
      final int c = subject.compareTo(other.subject);
      if (c == 0) {
        // TODO: improve this
        return sts.toString().compareTo(other.sts.toString());
      } else {
        return c;
      }
    }
    throw new ClassCastException("Comparing " + getClass().getName()
      + " with " + (o == null ? "null" : o.getClass().getName()));
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof MapEntityDescription) {
      final MapEntityDescription m = (MapEntityDescription) obj;
      return sts.equals(m.sts) && (subject == null ? m.subject == null : subject.equals(m.subject));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hash = 1;
    hash = 31 * hash + (subject == null ? 0 : subject.hashCode());
    hash = 31 * hash + sts.hashCode();
    return hash;
  }

  @Override
  public String toString() {
    return "subject=" + subject + " statements=" + sts.toString();
  }

}
