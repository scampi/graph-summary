/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.benchmark.cascading.properties;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
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
 * {@link PropertiesCount1} instances.
 * <p>
 * To use, call
 * <pre>
 * {@code TupleSerializationProps.addSerialization(properties, PropertiesCountSerialization1.class.getName())}
 * </pre>
 */
@SerializationToken(
  classNames={ "org.sindice.analytics.benchmark.cascading.properties.PropertiesCountDevelop1" },
  tokens={ 137 }
)
public class PropertiesCountSerialization1
extends Configured
implements Serialization<PropertiesCount1> {

  static class PropertiesCountDeserializer implements Deserializer<PropertiesCount1> {

    private DataInputStream  in;
    private final List<Long> datatypes = new ArrayList<Long>();

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
    public PropertiesCount1 deserialize(PropertiesCount1 t)
    throws IOException {
      final PropertiesCount1 pc;

      if (t == null) {
        pc = new PropertiesCount1();
      } else {
        pc = t;
      }
      datatypes.clear();
      datatypes.add(PropertiesCount1.NO_DATATYPE); // register the no-datatype hash
      pc.clear();

      final int size = WritableUtils.readVInt(in);
      for (int i = 0; i < size; i++) {
        final long label = WritableUtils.readVLong(in);

        final int nbDatatypes = WritableUtils.readVInt(in);
        final Map<Long, Long> map = new TreeMap<Long, Long>();
        for (int j = 0; j < nbDatatypes; j++) {
          final int index = WritableUtils.readVInt(in);
          final long datatype;
          if (index > datatypes.size()) {
            datatype = WritableUtils.readVLong(in);
            datatypes.add(datatype);
          } else {
            datatype = datatypes.get(index - 1);
          }
          final long count = WritableUtils.readVLong(in);
          map.put(datatype, count);
        }
        pc.put(label, map);
      }
      return pc;
    }

    @Override
    public void close()
    throws IOException {
      in.close();
    }

  }

  static class PropertiesCountSerializer implements Serializer<PropertiesCount1> {

    private DataOutputStream out;
    private final List<Long> datatypes = new ArrayList<Long>();

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
    public void serialize(PropertiesCount1 t)
    throws IOException {
      datatypes.clear();
      datatypes.add(PropertiesCount1.NO_DATATYPE); // register the no-datatype hash

      WritableUtils.writeVInt(out, t.size());
      for (Entry<Long, Map<Long, Long>> pair : t.entrySet()) {
        WritableUtils.writeVLong(out, pair.getKey());
        WritableUtils.writeVInt(out, pair.getValue().size());
        for (Entry<Long, Long> datatype : pair.getValue().entrySet()) {
          final int index = datatypes.indexOf(datatype.getKey());
          if (index == -1) {
            WritableUtils.writeVInt(out, datatypes.size() + 1); // add 1 to be able to deserialize correctly
            WritableUtils.writeVLong(out, datatype.getKey());
            datatypes.add(datatype.getKey());
          } else {
            WritableUtils.writeVInt(out, index + 1);
          }
          WritableUtils.writeVLong(out, datatype.getValue());
        }
      }
    }

    @Override
    public void close()
    throws IOException {
      out.close();
    }

  }

  public PropertiesCountSerialization1() {
  }

  @Override
  public boolean accept(Class<?> c) {
    return PropertiesCount1.class == c;
  }

  @Override
  public Serializer<PropertiesCount1> getSerializer(Class<PropertiesCount1> c) {
    return new PropertiesCountSerializer();
  }

  @Override
  public Deserializer<PropertiesCount1> getDeserializer(Class<PropertiesCount1> c) {
    return new PropertiesCountDeserializer();
  }

}
