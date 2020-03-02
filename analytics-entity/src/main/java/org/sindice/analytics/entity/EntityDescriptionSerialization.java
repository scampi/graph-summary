/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.entity;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.openrdf.model.URI;
import org.sindice.core.analytics.util.BytesRef;

import cascading.tuple.hadoop.SerializationToken;

/**
 * This class is an implementation of Hadoop's {@link Serialization} interface in order to serialize
 * {@link EntityDescription} instances.
 * <p>
 * To use, call
 * <pre>
 * {@code TupleSerializationProps.addSerialization(properties, EntityDescriptionSerialization.class.getName())}
 * </pre>
 */
@SerializationToken(
  classNames={ "org.sindice.analytics.entity.MapEntityDescription" },
  tokens={ 136 }
)
public class EntityDescriptionSerialization
extends Configured
implements Serialization<EntityDescription> {

  static class MapEntityDescriptionDeserializer implements Deserializer<EntityDescription> {

    private DataInputStream      in;

    /** {@link List} of the current prefixes. This lists gets updated as the entity is deserialized */
    private final List<BytesRef> prefixes = new ArrayList<BytesRef>();

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
    public EntityDescription deserialize(EntityDescription t)
    throws IOException {
      final MapEntityDescription entity;

      if (t == null) {
        entity = new MapEntityDescription();
      } else {
        entity = (MapEntityDescription) t;
      }

      entity.reset();
      prefixes.clear();
      // statements
      final int nbPreds = WritableUtils.readVInt(in);
      if (nbPreds != 0) {
        entity.subject = read(prefixes);
        for (int i = 0; i < nbPreds; i++) {
          final AnalyticsValue predicate = read(prefixes);
          final Set<AnalyticsValue> objects = new HashSet<AnalyticsValue>();
          final int nbObjects = WritableUtils.readVInt(in);
          for (int j = 0; j < nbObjects; j++) {
            objects.add(read(prefixes));
          }
          entity.sts.put(predicate, objects);
          entity.size += nbObjects;
        }
      }
      return entity;
    }

    /**
     * Deserializes either the {@link AnalyticsUri} or {@link AnalyticsLiteral}, expanding
     * the prefix of the {@link URI} or the {@link AnalyticsLiteral#getDatatype() datatype} using
     * the supplied list of prefixes.
     * @param prefixes the {@link List} of prefixes
     */
    private AnalyticsValue read(final List<BytesRef> prefixes)
    throws IOException {
      final BytesRef prefix;
      final int code = in.readByte();

      switch (code) {
        case AnalyticsValue.URI_CODE:
          final AnalyticsUri uri = new AnalyticsUri();
          prefix = getPrefix();
          uri.readFields(in, prefix == null ? 0 : prefix.length);
          System.arraycopy(prefix.bytes, prefix.offset, uri.getValue().bytes, 0, prefix.length);
          return uri;
        case AnalyticsValue.BNODE_CODE:
          final AnalyticsBNode bnode = new AnalyticsBNode();
          bnode.readFields(in);
          return bnode;
        case AnalyticsValue.LITERAL_CODE:
          final AnalyticsLiteral lit = new AnalyticsLiteral();
          prefix = getPrefix();

          if (prefix == null) {
            lit.readFields(in);
            return lit;
          }
          lit.readFields(in, prefix.length);
          final AnalyticsUri datatype = lit.getDatatypeAsAnalyticsURI();
          if (datatype.getValue().length == 0) {
            // no localname
            datatype.getValue().bytes = prefix.bytes;
            datatype.getValue().offset = prefix.offset;
            datatype.getValue().length = prefix.length;
          } else {
            System.arraycopy(prefix.bytes, prefix.offset, datatype.getValue().bytes, 0, prefix.length);
          }
          return lit;
        default:
          throw new IllegalArgumentException("Recieved unknown AnalyticsValue code: " + code);
      }
    }

    /**
     * Returns the prefix value, or <code>null</code> if there is none.
     * If the prefix used is not in the list, it is read from the {@link DataInput}.
     */
    private BytesRef getPrefix()
    throws IOException {
      final int index = WritableUtils.readVInt(in);
      final BytesRef prefix;

      if (index == 0) { // no prefix
        return null;
      }
      if (index > prefixes.size()) { // new prefix
        final int len = WritableUtils.readVInt(in);
        final byte[] b = new byte[len];
        in.readFully(b);
        prefix = new BytesRef(b);
        prefixes.add(prefix);
      } else { // get registered prefix
        prefix = prefixes.get(index - 1);
      }
      return prefix;
    }

    @Override
    public void close()
    throws IOException {
      in.close();
    }

  }

  static class MapEntityDescriptionSerializer implements Serializer<EntityDescription> {

    private DataOutputStream out;

    /** {@link List} of the current prefixes. This lists gets updated as the entity is serialized */
    private final List<BytesRef> prefixes = new ArrayList<BytesRef>();

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
    public void serialize(EntityDescription t)
    throws IOException {
      final MapEntityDescription entity = (MapEntityDescription) t;

      prefixes.clear();
      // statements
      WritableUtils.writeVInt(out, entity.sts.size());
      write(prefixes, entity.subject);
      for (Entry<AnalyticsValue, Set<AnalyticsValue>> pos : entity.sts.entrySet()) {
        write(prefixes, pos.getKey());
        WritableUtils.writeVInt(out, pos.getValue().size());
        for (AnalyticsValue object : pos.getValue()) {
          write(prefixes, object);
        }
      }
    }

    /**
     * Write the given {@link AnalyticsValue} to the {@link DataOutputStream} if non-<code>null</code>.
     * @param prefixes the current {@link List} of prefixes
     * @param value the {@link AnalyticsValue} to write
     * @throws IOException if the value could not be serialized
     */
    private void write(final List<BytesRef> prefixes, AnalyticsValue value)
    throws IOException {
      if (value == null) {
        return;
      }
      out.writeByte(value.getUniqueClassID());
      if (value instanceof AnalyticsUri) {
        write(prefixes, (AnalyticsUri) value, value);
      } else if (value instanceof AnalyticsLiteral) {
        final AnalyticsLiteral lit = (AnalyticsLiteral) value;
        final AnalyticsUri datatype = lit.getDatatypeAsAnalyticsURI();
        if (datatype.getValue().length != 0) {
          write(prefixes, datatype, value);
        } else {
          out.writeByte(0); // zero means no prefix for the datatype (i.e., datatype not set)
          value.write(out);
        }
      } else {
        value.write(out);
      }
    }

    /**
     * Writes the {@link AnalyticsUri} as a pair of prefix value and localname.
     * The prefix value is the index of the {@link AnalyticsUri#getNamespace() namespace} in the
     * prefixes lists.
     * @param prefixes the current {@link List} of prefixes
     * @param uri the {@link AnalyticsUri} to shorten with a prefix
     * @param value the {@link AnalyticsValue} to serialize, either an {@link AnalyticsLiteral}
     * or an {@link AnalyticsUri}
     * @throws IOException if the value failed to serialize
     */
    private void write(final List<BytesRef> prefixes, AnalyticsUri uri, AnalyticsValue value)
    throws IOException {
      final int offset = uri.getValue().offset;
      final int len = uri.getValue().length;

      uri.getValue().length = uri.getLocalNameIdx();

      final int index = prefixes.indexOf(uri.getValue());
      // add 1 to the index in order to encode "no prefix" at position 0
      if (index == -1) { // add the prefix
        WritableUtils.writeVInt(out, prefixes.size() + 1);
        WritableUtils.writeVInt(out, uri.getLocalNameIdx());
        out.write(uri.getValue().bytes, uri.getValue().offset, uri.getValue().length);
        prefixes.add(uri.getValue().clone());
      } else { // get the prefix
        WritableUtils.writeVInt(out, index + 1);
      }

      uri.getValue().offset = uri.getLocalNameIdx();
      uri.getValue().length = len - uri.getLocalNameIdx();
      value.write(out);

      uri.getValue().offset = offset;
      uri.getValue().length = len;
    }

    @Override
    public void close()
    throws IOException {
      out.close();
    }

  }

  public EntityDescriptionSerialization() {
  }

  @Override
  public boolean accept(Class<?> c) {
    return EntityDescription.class.isAssignableFrom(c);
  }

  @Override
  public Serializer<EntityDescription> getSerializer(Class c) {
    return new MapEntityDescriptionSerializer();
  }

  @Override
  public Deserializer<EntityDescription> getDeserializer(Class c) {
    return new MapEntityDescriptionDeserializer();
  }

}
