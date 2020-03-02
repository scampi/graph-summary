/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import cascading.flow.Flow;

/**
 * Utility class to load a Cascade configuration parameters from a YAML formatted input.
 * <p>
 * The entry in the YAML file named {@value #DEFAULT_PARAMETERS} defines the default set of parameters to be applied on
 * all the cascade {@link Flow}s.
 * <p>
 * Other entries in the YAML file configure a {@link Flow} in the cascade with the same name. Parameters defined there
 * overwrite the ones from the {@value #DEFAULT_PARAMETERS} entry.
 * <p>
 * Hadoop parameters (option staring with <b>-D</b>) defined in the <b>config.yaml</b> file have priority over
 * parameters passed from the CLI.
 */
public class CascadeConfYAMLoader {

  private static final Logger           logger             = LoggerFactory.getLogger(CascadeConfYAMLoader.class);

  public final static String            DEFAULT_PARAMETERS = "default";

  private final Yaml                    yaml               = new Yaml();
  private final Map<String, Properties> cascadeProperties  = new TreeMap<String, Properties>();
  final Properties                      defaultProperties  = new Properties();

  /**
   * Load configuration parameters from the YAML formatted input stream.
   * @param stream the YAML formatted file as an {@link InputStream}
   */
  public void load(final InputStream stream) {
    this.load(null, stream);
  }

  /**
   * Load configuration parameters from the YAML formatted input stream.
   * Parameters in the {@link Configuration} which key and value are {@link String}
   * overwrite the ones in the YAML input, if any.
   * Either arguments may be <code>null</code>.
   * @param cliConf the {@link Configuration} that comes from the {@link AbstractAnalyticsCLI}
   * @param stream the YAML formatted file as an {@link InputStream}
   */
  public void load(final Configuration cliConf,
                   final InputStream stream) {
    if (cliConf != null) {
      final Iterator<Entry<String, String>> it = cliConf.iterator();
      while (it.hasNext()) {
        final Entry<String, String> entry = it.next();
        defaultProperties.setProperty(entry.getKey(), entry.getValue());
        for (Properties flowProps: cascadeProperties.values()) {
          flowProps.remove(entry.getKey());
        }
      }
    }
    if (stream != null) {
      final Map<String, Object> flowConf = (Map) yaml.load(stream);

      // Get the declared parameters in the config file
      for (Entry<String, Object> c : flowConf.entrySet()) {
        final Properties properties = new Properties();
        if (c.getKey().equals(DEFAULT_PARAMETERS)) {
          parseFlowParameters(defaultProperties, c, false);
        } else {
          parseFlowParameters(properties, c, true);
          cascadeProperties.put(c.getKey(), properties);
        }
      }
    }
  }

  /**
   * Get the parameters and the associated values.
   * Multiple values associated to a same parameter are concatenated with a ','.
   * @param properties the {@link Properties} to store the parameters
   * @param parameters the {@link Map} of parameters extracted from the YAML file
   * @param withMulti if <code>true</code>, a property can be multi-valued
   */
  private void parseFlowParameters(final Properties properties,
                                   final Entry<String, Object> parameters,
                                   final boolean withMulti) {
    for (Map<String, Object> param : (List<Map<String, Object>>) parameters.getValue()) {
      for (Entry<String, Object> entry : param.entrySet()) {
        if (withMulti && properties.containsKey(entry.getKey())) {
          properties.setProperty(entry.getKey(), properties.getProperty(entry.getKey()) +
            "," + entry.getValue().toString());
        } else if (entry.getValue() instanceof List<?>) {
          final StringBuilder sb = new StringBuilder();
          for (Object value: (List<?>) entry.getValue()) {
            sb.append(value.toString()).append(",");
          }
          sb.deleteCharAt(sb.length() - 1);
          properties.setProperty(entry.getKey(), sb.toString());
        } else {
          properties.setProperty(entry.getKey(), entry.getValue().toString());
        }
      }
    }
  }

  /**
   * Return a {@link Map} of {@link Properties}, where a key correspond
   * to a flow name passed in argument.
   * @param flowsName the list of flows name to get properties for
   * @return a {@link Map} of {@link Properties}
   */
  public Map<String, Properties> getFlowsConfiguration(String...flowsName) {
    final HashSet<String> fnames = new HashSet<String>();

    if (flowsName == null || flowsName.length == 0) {
      final HashMap<String, Properties> def = new HashMap<String, Properties>();
      def.put(DEFAULT_PARAMETERS, defaultProperties);
      return def;
    }
    logger.info("Getting parameters for the flows: {}", Arrays.toString(flowsName));
    for (String fn : flowsName) {
      fnames.add(fn);
    }
    // Check flows name
    for (String name: cascadeProperties.keySet()) {
      if (!fnames.contains(name)) {
        throw new IllegalArgumentException("Unexpected flow name: " + name);
      }
    }
    // update the flows properties with the default ones
    for (String fn : fnames) {
      if (!cascadeProperties.containsKey(fn)) { // Add missing configuration
        final Properties p = new Properties();
        p.putAll(defaultProperties);
        cascadeProperties.put(fn, p);
      } else { // Add the default values to pre-configured flows
        for (Entry<Object, Object> defaultParameter : defaultProperties.entrySet()) {
          if (!cascadeProperties.get(fn).containsKey(defaultParameter.getKey())) {
            cascadeProperties.get(fn).setProperty(defaultParameter.getKey().toString(),
              defaultParameter.getValue().toString());
          }
        }
      }
    }
    return cascadeProperties;
  }

  /**
   * Returns a {@link Map} with the properties that were used in this Job.
   */
  Map<String, Properties> getCascadeProperties() {
    return cascadeProperties;
  }

}
