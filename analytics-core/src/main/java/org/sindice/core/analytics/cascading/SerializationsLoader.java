/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.io.serializer.Serialization;

import cascading.tuple.hadoop.TupleSerializationProps;

/**
 * The {@link SerializationsLoader} reads from resources named {@value #SERIALIZATIONS} the class
 * names of {@link Serialization}s that are to be added to the {@link TupleSerializationProps}.
 * <p>
 * The {@value #SERIALIZATIONS} files contains on each line a class name of a {@link Serialization} implementation.
 */
public class SerializationsLoader {

  private static Enumeration<URL> urls;

  /** The filename of the properties files that contain {@link Serialization}s class name */
  public static final String      SERIALIZATIONS = "serializations";

  /**
   * Sets the path to a serializations property file.
   */
  public static void setSerializations(final String path) {
    if (path == null) {
      throw new NullPointerException();
    }
    urls = new Enumeration<URL>() {
      private boolean hasMore = true;
      @Override
      public URL nextElement() {
        hasMore = false;
        try {
          return new File(path).toURI().toURL();
        } catch (MalformedURLException e) {
          throw new IllegalArgumentException("Unable to get file [" + path + "]", e);
        }
      }
      @Override
      public boolean hasMoreElements() {
        return hasMore;
      }
    };
  }

  /**
   * Returns an {@link Enumeration} over the serialization properties files.
   * If {@link #setSerializations(String)} was not called, this method searches the classpath
   * using {@link ClassLoader#getResources(String)}.
   */
  private static Enumeration<URL> getURLs() throws IOException {
    return urls == null ? SerializationsLoader.class.getClassLoader().getResources(SERIALIZATIONS) : urls;
  }

  /**
   * Reset the serializations resources.
   */
  public static void reset() {
    urls = null;
  }

  /**
   * Adds the Hadoop {@link Serialization}s from the {@link #getURLs() serializations resources} to
   * the given {@link Properties}.
   * @param properties the {@link Properties} to update with the serializations
   * @throws IOException if a serialization resource could not be read
   * @throws ClassNotFoundException if a class name in a {@value #SERIALIZATIONS} file was not found
   * @see TupleSerializationProps
   */
  public static void load(final Properties properties)
  throws IOException, ClassNotFoundException {
    final Enumeration<URL> urls = getURLs();
    final Set<String> classes = new HashSet<String>();

    while (urls.hasMoreElements()) {
      final URL url = urls.nextElement();
      final BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
      String line = null;

      try {
        while ((line = in.readLine()) != null) {
          if (line.isEmpty()) {
            continue;
          }
          classes.add(line);
          final Class<?> serial = SerializationsLoader.class.getClassLoader().loadClass(line);
          if (!Serialization.class.isAssignableFrom(serial)) {
            throw new IllegalArgumentException("Got a class that is not a Serialization: [" + line +
              "] in the file [" + url + "]");
          }
        }
      } finally {
        in.close();
      }
    }
    for (String c : classes) {
      TupleSerializationProps.addSerialization(properties, c);
    }

  }

}
