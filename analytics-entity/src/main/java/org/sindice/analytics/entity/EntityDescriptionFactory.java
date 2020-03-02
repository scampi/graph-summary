/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.entity;

import cascading.flow.FlowProcess;

/**
 * {@link EntityDescriptionFactory} allows to create {@link EntityDescription} instances based on the
 * {@link #ENTITY_DESCRIPTION} parameter of the {@link FlowProcess}.
 */
public class EntityDescriptionFactory {

  /**
   * Defines the {@link EntityDescription} implementation.
   * <p>
   * Default: {@link Type#MAP}
   * 
   * @see EntityDescription
   */
  public static final String ENTITY_DESCRIPTION = "entity-description";

  /**
   * The kind of {@link EntityDescription} implementation
   */
  public enum Type {
    /** {@link MapEntityDescription} implementation */
    MAP
  }

  /**
   * Returns a new {@link EntityDescription} instance, which {@link Type} is
   * set as the value of the {@link EntityParameters#ENTITY_DESCRIPTION} parameter.
   * The returned {@link EntityDescription} is {@link EntityDescription#setFlowProcess(FlowProcess) set}
   * with the given {@link FlowProcess}.
   * @param fp the {@link FlowProcess} containing relevant parameters for the {@link EntityDescription},
   * including {@link EntityParameters#ENTITY_DESCRIPTION}
   */
  public static EntityDescription getEntityDescription(FlowProcess fp) {
    final String v = fp.getStringProperty(ENTITY_DESCRIPTION);
    final Type type;

    if (v == null) {
      type = Type.MAP;
    } else {
      type = Type.valueOf(v);
    }

    final EntityDescription e;
    switch (type) {
      case MAP:
        e = new MapEntityDescription();
        break;
      default:
        throw new EnumConstantNotPresentException(Type.class, v);
    }
    e.setFlowProcess(fp);
    return e;
  }

}
