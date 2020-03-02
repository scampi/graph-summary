/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.hash2value.rdf.filter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.zip.GZIPOutputStream;

import org.sindice.core.analytics.util.AnalyticsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.pipe.assembly.AggregateBy.Functor;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Aggregate all tuples quads into a single tuple.
 * The aggregated RDF statements is Gzip compressed.
 */
public class GraphFilterAggregatePartials implements Functor {

  private static final Logger logger           = LoggerFactory.getLogger(GraphFilterAggregatePartials.class);
  private static final long   serialVersionUID = -830652416496668512L;
  private final Fields        declaredFields;

  public GraphFilterAggregatePartials(Fields declaredFields) {
    this.declaredFields = declaredFields;

    if (declaredFields.size() != 1) {
      throw new IllegalArgumentException(
        "declared fields can only have 1 fields, got: " + declaredFields);
    }
  }

  @Override
  public Tuple aggregate(FlowProcess flowProcess, TupleEntry args, Tuple context) {
    final String quad = args.getString("quad");
    final byte[] bytes = quad.getBytes(Charset.forName("UTF-8"));
    final GZIPOutputStream gzout;

    try {
      if (context == null) {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        gzout = new GZIPOutputStream(bos);
        context = new Tuple(gzout, bos, 0L);
      } else {
        gzout = (GZIPOutputStream) context.getObject(0);
        gzout.write(GraphFilterAggregateBy.NEWLINE);
      }
      gzout.write(bytes, 0, bytes.length);
      context.set(2, context.getLong(2) + 1);
    } catch(IOException e) {
      logger.error("", e);
      throw new RuntimeException(e);
    }
    return context;
  }

  @Override
  public Tuple complete(FlowProcess flowProcess, Tuple context) {
    final Tuple tuple = Tuple.size(1);

    final String name = GraphFilterAggregateBy.class.getSimpleName();
    final long size = context.getLong(2);
    AnalyticsUtil.incrementByRange(flowProcess, name, GraphFilterAggregateBy.NB_TRIPLES, size,
      50, 100, 500, 1000, 5000, 10000);

    try {
      ((GZIPOutputStream) context.getObject(0)).close();
    } catch (IOException e) {
      logger.error("", e);
      throw new RuntimeException(e);
    }
    tuple.set(0, ((ByteArrayOutputStream) context.getObject(1)).toByteArray());
    return tuple;
  }

  @Override
  public Fields getDeclaredFields() {
    return declaredFields;
  }

}
