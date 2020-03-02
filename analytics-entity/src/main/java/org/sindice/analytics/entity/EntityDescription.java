/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.entity;

import org.apache.hadoop.io.BytesWritable;
import org.openrdf.model.Value;
import org.sindice.core.analytics.util.Hash;

import cascading.flow.FlowProcess;


/**
 * This interface defines the high-level API for manipulating the
 * description of an entity.
 * 
 * <p>
 * 
 * The description of an entity consists in the data that describes an entity.
 * By default, the description is the set of outgoing edges of a resource,
 * i.e., the predicate and the associated object.
 * 
 * <p>
 * 
 * The {@link Statements} interface allows to iterate over the statements of the
 * description of the entity.
 * 
 * @see MapEntityDescription
 * @see EntityDescriptionSerialization
 * @see EntityDescriptionFactory
 */
public interface EntityDescription extends Comparable<EntityDescription> {

  /** Counter for the number of predicate-object pairs */
  public static final String PO_COUNTER   = "PO_";

  /**
   * Sets the {@link FlowProcess}. By default, the {@link FlowProcess} of this entity is {@link FlowProcess#NULL}.
   * @param fp the {@link FlowProcess} to set
   */
  public void setFlowProcess(FlowProcess fp);

  /**
   * Returns the entity identifier, i.e., the subject common to all the statements.
   * <p>
   * This method can return a <code>null</code> {@link AnalyticsValue} if an error
   * occurred while getting the entity value.
   */
  public AnalyticsValue getEntity();

  /**
   * Returns the actual number of statements added for the entity.
   */
  public int getNbStatements();

  /**
   * Provides a {@link Statements} in order to iterate over the description
   * of the entity.
   */
  public Statements iterateStatements();

  /**
   * Iterator over the statements of the entity
   */
  public interface Statements {

    /**
     * Returns <code>true</code> if there is a next statement
     */
    public boolean getNext();

    /**
     * Returns the predicate {@link Value}
     */
    public AnalyticsValue getPredicate();

    /**
     * Returns a copy of the predicate {@link Value}
     */
    public AnalyticsValue getPredicateCopy();

    /**
     * Returns the label of the predicate, hashed with {@link Hash#getHash64(String)}
     */
    public long getPredicateHash64();

    /**
     * Returns the object {@link Value}
     */
    public AnalyticsValue getObject();

    /**
     * Returns a copy of the object {@link Value}
     */
    public AnalyticsValue getObjectCopy();

    /**
     * Returns the label of the object, hashed with {@link Hash#getHash64(String)}.
     * 
     */
    public long getObjectHash64();

    /**
     * Returns the label of the object, hashed with {@link Hash#getHash128(Object...)}
     * 
     * <p>
     * 
     * This should be used to uniquely identify entities, since the probability
     * of collision is small.
     */
    public BytesWritable getObjectHash128();

  }

  /**
   * Reset the {@link EntityBuilder} instance to start collecting
   * statements about a new entity.
   */
  public void reset();

  /**
   * Add a statement about the entity.
   * Implementations should take care of not adding duplicates to the entity description.
   * 
   * @param s the subject {@link Value}
   * @param p the predicate {@link Value}
   * @param o the object {@link Value}
   */
  public void add(AnalyticsValue s, AnalyticsValue p, AnalyticsValue o);

  /**
   * Adds all the statements of the given entity to this entity.
   * The statements of given entity are not copied.
   * @param entity the {@link EntityDescription} to get the statements from
   */
  public void add(EntityDescription entity);

}
