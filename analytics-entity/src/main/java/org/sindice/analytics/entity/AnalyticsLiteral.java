/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.analytics.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.Arrays;

import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.sindice.core.analytics.util.BytesRef;

/**
 * This {@link Literal} implementation allows to set the label.
 */
public class AnalyticsLiteral
extends AnalyticsValue
implements Literal {

  private static final long  serialVersionUID = 1690508893533451205L;

  /** Flag for indicating that the {@link AnalyticsLiteral} has no tag during serialization. */
  public static final int    NO_TAG           = 0;

  /** Flag for indicating that the {@link AnalyticsLiteral} has a datatype tag during serialization. */
  public static final int    DATATYPE         = 1;

  /** Flag for indicating that the {@link AnalyticsLiteral} has a language tag during serialization. */
  public static final int    LANGUAGE         = 2;

  /**
   * The literal's language tag (<code>null</code> if not applicable).
   */
  private byte[]             language;

  /**
   * The literal's datatype (<code>null</code> if not applicable).
   */
  private final AnalyticsUri datatype         = new AnalyticsUri();

  /**
   * Create an {@link AnalyticsLiteral} instance with an empty label and no tag set.
   */
  public AnalyticsLiteral() {
    super();
  }

  /**
   * Create an {@link AnalyticsLiteral} instance with the given label (without the quotes) and no tag set.
   */
  public AnalyticsLiteral(String label) {
    super(label.getBytes(Charset.forName("UTF-8")));
  }

  /**
   * Create an {@link AnalyticsLiteral} instance with the given label (without the quotes) and the language and
   * datatype tags.
   * @throws IllegalArgumentException if both tags are set
   */
  public AnalyticsLiteral(String label, String datatype, String language) {
    this(label.getBytes(Charset.forName("UTF-8")),
      (datatype == null) ? BytesRef.EMPTY_BYTES : datatype.getBytes(Charset.forName("UTF-8")),
      (language == null) ? null : language.getBytes(Charset.forName("UTF-8")));
  }

  /**
   * Create an {@link AnalyticsLiteral} instance with the given label (without the quotes) and the language and
   * datatype tags.
   * @throws IllegalArgumentException if both tags are set
   */
  public AnalyticsLiteral(byte[] label, byte[] datatype, byte[] language) {
    super(label);
    if (datatype.length != 0 && language != null) {
      throw new IllegalArgumentException("A Literal can have only one tag. Got datatype=[" + datatype
        + "] and language=[" + language + "]");
    }
    this.datatype.setValue(datatype);
    this.language = language;
  }

  /**
   * Create an {@link AnalyticsLiteral} instance with the given label (without the quotes) and no tag set.
   * The value starts at the position offset, for length bytes.
   */
  public AnalyticsLiteral(byte[] value, int offset, int length) {
    super(value, offset, length);
  }

  @Override
  public void reset() {
    super.reset();
    datatype.reset();
    language = null;
  }

  @Override
  public AnalyticsValue getCopy() {
    final AnalyticsLiteral lit = new AnalyticsLiteral(value.bytes, value.offset, value.length);
    lit.setDatatypeBytes(datatype.value.bytes, datatype.value.offset, datatype.value.length);
    lit.setLanguageBytes(language);
    return lit;
  }

  @Override
  public String getLabel() {
    return stringValue();
  }

  @Override
  public String getLanguage() {
    return language == null ? null : new String(language, Charset.forName("UTF-8"));
  }

  @Override
  public URI getDatatype() {
    return datatype.getValue().length == 0 ? null : datatype;
  }

  /**
   * Returns the datatype as an {@link AnalyticsUri}.
   * If the {@link BytesRef#length length} is <code>0</code>, there is no datatype tag set.
   */
  public AnalyticsUri getDatatypeAsAnalyticsURI() {
    return datatype;
  }

  /**
   * @param datatype the datatype to set
   */
  public void setDatatypeBytes(byte[] datatype, int offset, int length) {
    if (datatype == null || length == 0) {
      this.datatype.reset();
    } else {
      this.datatype.value.bytes = datatype;
      this.datatype.value.offset = offset;
      this.datatype.value.length = length;
    }
  }

  /**
   * @param language the language to set
   */
  public void setLanguageBytes(byte[] language) {
    if (language == null || language.length == 0) {
      this.language = null;
    } else {
      this.language = language;
    }
  }

  public byte[] getLanguageBytes() {
    return language;
  }

  @Override
  public byte getUniqueClassID() {
    return 2;
  }

  @Override
  public void write(DataOutput out)
  throws IOException {
    super.write(out);

    // tag
    if (language != null) {
      out.writeByte(LANGUAGE);
      WritableUtils.writeVInt(out, language.length);
      out.write(language);
    } else if (datatype.value.length != 0) {
      out.writeByte(DATATYPE);
      WritableUtils.writeVInt(out, datatype.value.length);
      out.write(datatype.value.bytes, datatype.value.offset, datatype.value.length);
    } else {
      out.writeByte(NO_TAG);
    }
  }

  @Override
  public void readFields(DataInput in, int minLength)
  throws IOException {
    reset();
    setValue(getValue(in, 0));

    final int code = in.readByte();
    switch (code) {
      case NO_TAG:
        break;
      case DATATYPE:
        final byte[] dt = getValue(in, minLength);
        setDatatypeBytes(dt, 0, dt.length);
        break;
      case LANGUAGE:
        setLanguageBytes(getValue(in, 0));
        break;
      default:
        throw new IllegalArgumentException("Received unknown Literal code = [" + code + "]");
    }
  }

  @Override
  public byte byteValue() {
    throw new NotImplementedException();
  }

  @Override
  public short shortValue() {
    return 0;
  }

  @Override
  public int intValue() {
    throw new NotImplementedException();
  }

  @Override
  public long longValue() {
    throw new NotImplementedException();
  }

  @Override
  public BigInteger integerValue() {
    throw new NotImplementedException();
  }

  @Override
  public BigDecimal decimalValue() {
    throw new NotImplementedException();
  }

  @Override
  public float floatValue() {
    throw new NotImplementedException();
  }

  @Override
  public double doubleValue() {
    throw new NotImplementedException();
  }

  @Override
  public boolean booleanValue() {
    throw new NotImplementedException();
  }

  @Override
  public XMLGregorianCalendar calendarValue() {
    throw new NotImplementedException();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();

    sb.append('"');
    sb.append(stringValue());
    sb.append('"');

    if (language != null) {
      sb.append('@');
      sb.append(new String(language, Charset.forName("UTF-8")));
    }

    if (datatype.value.length != 0) {
      sb.append("^^<");
      sb.append(datatype.stringValue());
      sb.append(">");
    }
    return sb.toString();
  }

  @Override
  public boolean equals(Object object) {
    if (super.equals(object)) {
      final AnalyticsLiteral lit = (AnalyticsLiteral) object;

      // Compare datatypes
      if (!datatype.equals(lit.datatype)) {
        return false;
      }

      // Compare language tags
      if (language == null) {
        if (lit.getLanguageBytes() != null) {
          return false;
        }
      } else {
        if (!Arrays.equals(language, lit.getLanguageBytes())) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public int compareTo(AnalyticsValue o) {
    int c = super.compareTo(o);

    if (c == 0) {
      if (datatype != null) {
        c = datatype.compareTo(((AnalyticsLiteral) o).getDatatypeAsAnalyticsURI());
        if (c != 0) {
          return c;
        }
      }
      if (language != null) {
        c = WritableComparator.compareBytes(language, 0, language.length,
          ((AnalyticsLiteral) o).getLanguageBytes(), 0, ((AnalyticsLiteral) o).getLanguageBytes().length);
        if (c != 0) {
          return c;
        }
      }
      return 0;
    }
    return c;
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + datatype.hashCode();
    hash = 31 * hash + Arrays.hashCode(language);
    return hash;
  }

}
