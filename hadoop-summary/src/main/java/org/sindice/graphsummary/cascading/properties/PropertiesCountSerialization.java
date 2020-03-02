/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.properties;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

import cascading.tuple.hadoop.SerializationToken;

/**
 * This class is an implementation of Hadoop's {@link Serialization} interface in order to serialize
 * {@link PropertiesCount} instances.
 * <p>
 * To use, call
 * <pre>
 * {@code TupleSerializationProps.addSerialization(properties, PropertiesCountSerialization.class.getName())}
 * </pre>
 */
@SerializationToken(
  classNames={ "org.sindice.graphsummary.cascading.properties.PropertiesCount" },
  tokens={ 137 }
)
public class PropertiesCountSerialization
extends Configured
implements Serialization<PropertiesCount> {

  static class PropertiesCountDeserializer implements Deserializer<PropertiesCount> {

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
    public PropertiesCount deserialize(PropertiesCount t)
    throws IOException {
      final PropertiesCount pc;

      if (t == null) {
        pc = new PropertiesCount();
      } else {
        pc = t;
      }
      datatypes.clear();
      datatypes.add(PropertiesCount.NO_DATATYPE); // register the no-datatype hash
      pc.clear();

      final int size = WritableUtils.readVInt(in);
      for (int i = 0; i < size; i++) {
        final long label = in.readLong();
        final int nbDatatypes = WritableUtils.readVInt(in);

        // The datatypes are serialized in order
        final List<Long> dt = new ArrayList<Long>();
        for (int j = 0; j < nbDatatypes; j++) {
          final int index = WritableUtils.readVInt(in);
          final long datatype;
          if (index >= datatypes.size()) {
            datatype = in.readLong();
            datatypes.add(datatype);
          } else {
            datatype = datatypes.get(index);
          }
          final long count = WritableUtils.readVLong(in);
          dt.add(datatype);
          dt.add(count);
        }
        pc.ptrs.put(label, dt);
      }
      return pc;
    }

    @Override
    public void close()
    throws IOException {
      in.close();
    }

  }

  static class PropertiesCountSerializer implements Serializer<PropertiesCount> {

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
    public void serialize(PropertiesCount t)
    throws IOException {
      datatypes.clear();
      datatypes.add(PropertiesCount.NO_DATATYPE); // register the no-datatype hash

      WritableUtils.writeVInt(out, t.size());
      for (Entry<Long, List<Long>> entry : t.ptrs.entrySet()) {
        final List<Long> odt = entry.getValue();

        out.writeLong(entry.getKey());
        WritableUtils.writeVInt(out, odt.size() / 2);
        for (int j = 0; j < odt.size(); j += 2) {
          final long datatype = odt.get(j);
          final long count = odt.get(j + 1);

          final int index = datatypes.indexOf(datatype);
          if (index == -1) {
            WritableUtils.writeVInt(out, datatypes.size());
            out.writeLong(datatype);
            datatypes.add(datatype);
          } else {
            WritableUtils.writeVInt(out, index);
          }
          WritableUtils.writeVLong(out, count);
        }
      }
    }

    @Override
    public void close()
    throws IOException {
      out.close();
    }

  }

  public PropertiesCountSerialization() {
  }

  @Override
  public boolean accept(Class<?> c) {
    return PropertiesCount.class == c;
  }

  @Override
  public Serializer<PropertiesCount> getSerializer(Class<PropertiesCount> c) {
    return new PropertiesCountSerializer();
  }

  @Override
  public Deserializer<PropertiesCount> getDeserializer(Class<PropertiesCount> c) {
    return new PropertiesCountDeserializer();
  }

}
