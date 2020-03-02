/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.representer.Representer;
import org.yaml.snakeyaml.resolver.Resolver;

import cascading.cascade.Cascade;

/**
 * This class loads the set of graph summary cascade configurations.
 * <p>
 * The file <a href="src/main/resources/graph-summary_conf.yaml">{@value #GRAPH_SUMMARY_CONF}</a> defines the possible
 * configurations of the graph summarization {@link Cascade}.
 * <p>
 * Each configuration is defined uniquely using the attribute named <b>{@value #CONF_NAME}</b>. If that attribute value is not
 * unique, an {@link IllegalArgumentException} is thrown.
 * <p>
 * A pre-defined configuration can be retrieved through {@link #get(String, String)}, where the arguments matches
 * a key-value pair of the desired configuration. If several configurations are matched,
 * an {@link IllegalArgumentException} is thrown. If the key-value pair is not found, the default configuration
 * with <b> {@link #CONF_NAME} </b> equal to <b> {@link #CONF_NAME_DEFAULT} </b> is returned.
 * <p>
 * If a property is not set in a specific configuration, but is in the default one, then the value
 * from the default configuration is used.
 */
public class GraphSummaryConfig {

  private static final Logger logger = LoggerFactory.getLogger(GraphSummaryConfig.class);

  /** The default filename of the YAML configuration file */
  private final static String GRAPH_SUMMARY_CONF = "graph-summary_conf.yaml";
  private static String confFile = null;

  /** This attribute of a configuration uniquely defines the configuration */
  public static final String CONF_NAME = "conf-name";

  /** The name of the default configuration */
  public static final String CONF_NAME_DEFAULT = "default";

  /** The list of configurations */
  private static List<Map<String, String>> configs;

  private static Map<String, String> defaultConf;

  private GraphSummaryConfig() {}

  /**
   * The path to the file containing the list of configurations in the YAML format.
   * @throws NullPointerException if confFile is <code>null</code>
   */
  public static void setConfFile(String confFile) {
    if (confFile == null) {
      throw new NullPointerException();
    }
    GraphSummaryConfig.confFile = confFile;
  }

  /**
   * Reset the configuration so that it has to read from the sources.
   */
  public static void reset() {
    configs = null;
    confFile = null;
    defaultConf = null;
  }

  /**
   * Return an {@link InputStream} from the graph summary configuration file.
   */
  private static InputStream getConfStream()
  throws IOException {
    if (confFile != null) {
      return new FileInputStream(confFile);
    }
    final InputStream stream = GraphSummaryConfig.class.getClassLoader().getResourceAsStream(GRAPH_SUMMARY_CONF);
    if (stream == null) {
      throw new IllegalArgumentException("The resource graph-summary_conf.yaml could not be found");
    }
    return stream;
  }

  /**
   * Load the graph summary configuration file.
   */
  private static void init()
  throws IOException {
    if (configs == null) {
      final Yaml yaml = new Yaml(new Constructor(), new Representer(), new DumperOptions(), new Resolver(false));
      final Set<String> set = new HashSet<String>();

      // Get the default config
      for (Object o : yaml.loadAll(getConfStream())) {
        final Map<String, String> map = (Map<String, String>) o;
        final String name = map.get(CONF_NAME);
        if (name == null) {
          throw new IllegalArgumentException("Found config without a conf-name: " + map);
        }
        if (name.equals(CONF_NAME_DEFAULT)) {
          defaultConf = map;
          break;
        }
      }
      if (defaultConf == null) {
        throw new IllegalArgumentException("Missing default configuration");
      }

      configs = new ArrayList<Map<String,String>>();
      for (Object o : yaml.loadAll(getConfStream())) {
        final Map<String, String> map = (Map<String, String>) o;
        final String name = map.get(CONF_NAME);
        if (name.equals(CONF_NAME_DEFAULT)) {
          continue;
        }
        if (!set.add(name)) {
          throw new IllegalArgumentException("The configuration file " + confFile +
            " contains more than one configuration with same conf-name");
        }
        // set the default values, if unset
        for (Entry<String, String> entry : defaultConf.entrySet()) {
          if (!map.containsKey(entry.getKey())) {
            map.put(entry.getKey(), entry.getValue());
          }
        }
        configs.add(Collections.unmodifiableMap(map));
      }
    }
  }

  /**
   * Return the graph summary configuration identified by the given pair of parameter/value.
   * @param key The parameter name
   * @param value the parameter value
   * @return the {@link Map} describing the configuration of the graph summary
   * @throws IOException if an error occured while reading the configuration file
   * @throws IllegalArgumentException if the configuration file is not valid.
   */
  public static Map<String, String> get(String key, String value)
  throws IOException {
    if (key == null || value == null) {
      throw new NullPointerException("The key nor the value to be searched for cannot be null");
    }
    init();

    Map<String, String> wantedConf = null;
    for (Map<String, String> conf : configs) {
      final Object v = conf.get(key);
      if (v != null && v.equals(value)) {
        if (wantedConf == null) {
          wantedConf = conf;
        } else {
          throw new IllegalArgumentException("More than one configuration is matched by the pair " + key + "=" + value);
        }
      }
    }
    if (wantedConf == null) {
      logger.info("The pair {}={} was not found in the configuration file," +
          "taking the default configuration instead", key, value);
      return defaultConf;
    }
    return wantedConf;
  }

}
