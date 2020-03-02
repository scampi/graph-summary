/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.sindice.core.analytics.util.BytesRef;

/**
 * This {@link Value} allows the value of either a {@link URI}, a {@link Literal}, or a {@link BNode} instance to
 * be set thanks to {@link #setValue(byte[])}. Each value is encoded using the <tt>UTF-8</tt> encoding.
 */
public abstract class AnalyticsValue
implements Value, Comparable<AnalyticsValue>, Writable {

  private static final long serialVersionUID = -6184888041708310403L;

  /** Code to represent a {@link URI} */
  public final static byte  URI_CODE         = 0;
  /** Code to represent a {@link BNode} */
  public final static byte  BNODE_CODE       = 1;
  /** Code to represent a {@link Literal} */
  public final static byte  LITERAL_CODE     = 2;

  protected final BytesRef  value;

  public AnalyticsValue() {
    value = new BytesRef();
  }

  public AnalyticsValue(byte[] value) {
    if (value == null) {
      throw new NullPointerException();
    }
    this.value = new BytesRef(value);
  }

  public AnalyticsValue(byte[] value, int offset, int length) {
    if (value == null) {
      throw new NullPointerException();
    }
    this.value = new BytesRef(value, offset, length);
  }

  /**
   * Creates an {@link AnalyticsValue} from a given Sesame's {@link Value} instance.
   */
  public static AnalyticsValue fromSesame(Value value) {
    return fromSesame(value, null);
  }

  /**
   * Creates an {@link AnalyticsValue} from a given Sesame's {@link Value} instance.
   * The <b>values</b> argument contains the possible reusable instances of the sesame value, in the following order:
   * {@link AnalyticsUri}, then {@link AnalyticsBNode}, and finally {@link AnalyticsLiteral}.
   * This argument can be <code>null</code>, in which case a new {@link AnalyticsValue} instance is created.
   */
  public static AnalyticsValue fromSesame(Value value, AnalyticsValue[] values) {
    final String label = value.stringValue();

    if (value instanceof URI) {
      final AnalyticsUri uri = (AnalyticsUri) (values == null ? new AnalyticsUri() : values[0]);
      uri.reset();
      uri.setValue(label);
      return uri;
    } else if (value instanceof BNode) {
      final AnalyticsBNode bnode = (AnalyticsBNode) (values == null ? new AnalyticsBNode() : values[1]);
      bnode.reset();
      bnode.setValue(label);
      return bnode;
    } else if (value instanceof Literal) {
      final Literal litSesame = (Literal) value;
      final URI datatype = litSesame.getDatatype();
      final String lang = litSesame.getLanguage();
      final AnalyticsLiteral lit = (AnalyticsLiteral) (values == null ? new AnalyticsLiteral() : values[2]);
      lit.reset();
      lit.setValue(label);
      if (datatype == null) {
        lit.setDatatypeBytes(null, 0, 0);
      } else {
        final byte[] dt = datatype.stringValue().getBytes(Charset.forName("UTF-8"));
        lit.setDatatypeBytes(dt, 0, dt.length);
      }
      lit.setLanguageBytes(lang == null ? null : lang.getBytes(Charset.forName("UTF-8")));
      return lit;
    } else {
      throw new IllegalArgumentException("Unable to create an AnalyticsValue from a " +
        (value == null ? "null" : value.getClass().getName()));
    }
  }

  /**
   * Reset this {@link AnalyticsValue} in order to be reused.
   */
  public void reset() {
    value.offset = 0;
    value.length = 0;
  }

  @Override
  public final String stringValue() {
    return value.asUTF8();
  }

  /**
   * Sets the label of this {@link Value}.
   * The {@link BytesRef} offset is set to <code>0</code> and its length to the length of the given array.
   * @throws NullPointerException if value is <code>null</code>
   */
  public void setValue(byte[] value) {
    if (value == null) {
      throw new NullPointerException();
    }
    this.value.bytes = value;
    this.value.offset = 0;
    this.value.length = value.length;
  }

  /**
   * Sets the label of this {@link Value}.
   * The {@link BytesRef} offset is set to <code>0</code> and its length to the length of the given {@link BytesWritable}.
   * @throws NullPointerException if value is <code>null</code>
   */
  public void setValue(BytesWritable value) {
    if (value == null) {
      throw new NullPointerException();
    }
    this.value.bytes = value.getBytes();
    this.value.offset = 0;
    this.value.length = value.getLength();
  }

  /**
   * Sets the label of this {@link Value}.
   * @throws NullPointerException if value is <code>null</code>
   */
  public void setValue(String value) {
    if (value == null) {
      throw new NullPointerException();
    }
    if (value.length() == 0) {
      this.value.offset = 0;
      this.value.length = 0;
    } else {
      final byte[] bytes = value.getBytes(Charset.forName("UTF-8"));
      this.value.bytes = bytes;
      this.value.offset = 0;
      this.value.length = bytes.length;
    }
  }

  /**
   * Returns the label of this {@link Value} as a byte array.
   */
  public BytesRef getValue() {
    return value;
  }

  /**
   * Returns a copy of this {@link Value}.
   */
  public abstract AnalyticsValue getCopy();

  /**
   * Returns a unique identifier for this value's class.
   */
  public abstract byte getUniqueClassID();

  /**
   * Returns a {@link BytesWritable} wrapping the {@link #getValue() value} of this instance.
   * The value byte[] array is not copied.
   */
  public BytesWritable getValueAsBytesWritable() {
    final BytesWritable bytes = new BytesWritable(value.bytes);

    /*
     * #getTypedValue() appends a byte to the value, so the valid bytes range from 0 for valid.length bits.
     */
    bytes.setSize(value.length);
    return bytes;
  }

  @Override
  public void write(DataOutput out)
  throws IOException {
    WritableUtils.writeVInt(out, value.length);
    out.write(value.bytes, value.offset, value.length);
  }

  @Override
  public void readFields(DataInput in)
  throws IOException {
    readFields(in, 0);
  }

  /**
   * Deserializes the value.
   * <p>
   * If t is an {@link AnalyticsUri} or {@link AnalyticsLiteral}, allocate at least
   * <b>minLength</b> bytes for the {@link AnalyticsUri} or the datatype
   * <code>byte[]</code> array. The data is put at the end of the array, in order to
   * make space for a prefix value.
   * 
   * @param in DataInput to deseriablize this object from
   * @param minLength the minimum number of bytes to allocate
   * @throws IOException if the serialized value could not be read
   */
  public void readFields(DataInput in, int minLength)
  throws IOException {
    reset();
    setValue(getValue(in, minLength));
  }

  /**
   * Returns the byte[] value of the serialized {@link AnalyticsValue}.
   * @param in DataInput to deseriablize this object from
   * @param minLength the minimum number of bytes to allocate for this value.
   */
  protected byte[] getValue(DataInput in, final int minLength)
  throws IOException {
    final int len = minLength + WritableUtils.readVInt(in);

    if (len == 0) {
      return BytesRef.EMPTY_BYTES;
    }
    final byte[] value = new byte[len];
    in.readFully(value, minLength, len - minLength);
    return value;
  }

  @Override
  public int compareTo(AnalyticsValue o) {
    final int c = (getUniqueClassID() < o.getUniqueClassID()) ? -1 : ((getUniqueClassID() == o.getUniqueClassID()) ? 0 : 1);
    if (c == 0) {
      return value.compareTo(o.value);
    }
    return c;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof AnalyticsValue)) {
      return false;
    }
    if (getUniqueClassID() != ((AnalyticsValue) object).getUniqueClassID()) {
      return false;
    }
    return value.equals(((AnalyticsValue) object).value);
  }

  @Override
  public int hashCode() {
    int hash = 31 + value.hashCode();
    hash = 31 * hash + getUniqueClassID();
    return hash;
  }

}
