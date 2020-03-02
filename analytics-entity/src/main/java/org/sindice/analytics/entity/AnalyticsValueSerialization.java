/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.entity;

import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.WritableSerialization;

import cascading.tuple.hadoop.SerializationToken;

/**
 * This class is an implementation of Hadoop's {@link Serialization} interface in order to serialize
 * {@link AnalyticsValue} instances.
 * <p>
 * To use, call
 * <pre>
 * {@code TupleSerializationProps.addSerialization(properties, AnalyticsValueSerialization.class.getName())}
 * </pre>
 */
@SerializationToken(
  classNames = { "org.sindice.analytics.entity.AnalyticsUri",
                 "org.sindice.analytics.entity.AnalyticsBNode",
                 "org.sindice.analytics.entity.AnalyticsLiteral" },
  tokens = { 133, 134, 135 }
)
public class AnalyticsValueSerialization
extends WritableSerialization {

}
