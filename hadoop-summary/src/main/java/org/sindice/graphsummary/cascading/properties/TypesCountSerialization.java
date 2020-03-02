/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.properties;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

import cascading.tuple.hadoop.SerializationToken;

/**
 * This class is an implementation of Hadoop's {@link Serialization} interface in order to serialize
 * {@link TypesCount} instances.
 * <p>
 * To use, call
 * <pre>
 * {@code TupleSerializationProps.addSerialization(properties, TypesCountSerialization.class.getName())}
 * </pre>
 */
@SerializationToken(
  classNames={ "org.sindice.graphsummary.cascading.properties.TypesCount" },
  tokens={ 138 }
)
public class TypesCountSerialization
extends Configured
implements Serialization<TypesCount> {

  static class TypesCountDeserializer implements Deserializer<TypesCount> {

    private DataInputStream in;

    @Override
    public void open(InputStream in)
    throws IOException {
      if(in instanceof DataInputStream) {
        this.in = (DataInputStream) in;
      } else {
        this.in = new DataInputStream(in);
      }
    }

    @Override
    public TypesCount deserialize(TypesCount t)
    throws IOException {
      final TypesCount tc;

      if (t == null) {
        tc = new TypesCount();
      } else {
        tc = t;
      }
      tc.clear();
      final int size = WritableUtils.readVInt(in);
      for (int i = 0; i < size; i++) {
        final long type = WritableUtils.readVLong(in);

        final int nbPreds = WritableUtils.readVInt(in);
        final Map<Byte, Long> preds = new TreeMap<Byte, Long>();
        for (int j = 0; j < nbPreds; j++) {
          final byte key = in.readByte();
          final long value = WritableUtils.readVLong(in);
          preds.put(key, value);
        }
        tc.put(type, preds);
      }
      return tc;
    }

    @Override
    public void close()
    throws IOException {
      in.close();
    }

  }

  static class TypesCountSerializer implements Serializer<TypesCount> {

    private DataOutputStream out;

    @Override
    public void open(OutputStream out)
    throws IOException {
      if(out instanceof DataOutputStream) {
        this.out = (DataOutputStream) out;
      } else {
        this.out = new DataOutputStream(out);
      }
    }

    @Override
    public void serialize(TypesCount t)
    throws IOException {
      WritableUtils.writeVInt(out, t.size());
      for (Entry<Long, Map<Byte, Long>> pair : t.entrySet()) {
        WritableUtils.writeVLong(out, pair.getKey());
        WritableUtils.writeVInt(out, pair.getValue().size());
        for (Entry<Byte, Long> predicate : pair.getValue().entrySet()) {
          out.writeByte(predicate.getKey());
          WritableUtils.writeVLong(out, predicate.getValue());
        }
      }
    }

    @Override
    public void close()
    throws IOException {
      out.close();
    }

  }

  public TypesCountSerialization() {
  }

  @Override
  public boolean accept(Class<?> c) {
    return TypesCount.class == c;
  }

  @Override
  public Serializer<TypesCount> getSerializer(Class<TypesCount> c) {
    return new TypesCountSerializer();
  }

  @Override
  public Deserializer<TypesCount> getDeserializer(Class<TypesCount> c) {
    return new TypesCountDeserializer();
  }

}
