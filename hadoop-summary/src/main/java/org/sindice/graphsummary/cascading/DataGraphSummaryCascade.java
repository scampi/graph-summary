/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.mapred.JobConf;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.scheme.ExtensionTextLine;
import org.sindice.core.analytics.cascading.scheme.HFileScheme;
import org.sindice.core.analytics.cascading.statistics.CountersManager;
import org.sindice.core.analytics.util.AnalyticsException;
import org.sindice.graphsummary.cascading.ecluster.ClusterSubAssembly;
import org.sindice.graphsummary.cascading.ecluster.ClusteringFactory;
import org.sindice.graphsummary.cascading.ecluster.ClusteringFactory.ClusteringAlgorithm;
import org.sindice.graphsummary.cascading.entity.GetClusterGraph;
import org.sindice.graphsummary.cascading.entity.GetClusterGraph.PreProcessing;
import org.sindice.graphsummary.cascading.hash2value.GenerateDictionary;
import org.sindice.graphsummary.cascading.hash2value.rdf.RDFMetadataGraph;
import org.sindice.graphsummary.cascading.hash2value.rdf.RDFRelationsGraph;
import org.sindice.graphsummary.cascading.properties.GetPropertiesGraph;
import org.sindice.graphsummary.cascading.properties.GetPropertiesGraph.PropertiesProcessing;
import org.sindice.graphsummary.cascading.relationships.RelationsGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.TextLine.Compress;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

/**
 * This class creates the {@link Cascade} of the graph summarization, taking RDF data as input and generating
 * a summary of it.
 * <p>
 * This {@link Cascade} is parameterized with the following:
 * <ul>
 * <li><b> {@link ClusteringAlgorithm} </b>: the clustering algorithm to apply;</li>
 * <li><b> {@link PreProcessing} </b>: the kind of entity description desired, e.g., outgoing edges only,
 * outgoing + incoming edges, ... .</li>
 * </ul>
 * The file <a href="src/main/resources/graph-summary_conf.yaml">graph-summary_conf.yaml</a> defines the possible
 * configurations of the graph summarization {@link Cascade}.
 * 
 * @see GraphSummaryConfig
 */
@SuppressWarnings("rawtypes")
public class DataGraphSummaryCascade {

  public static final String   CASCADE_NAME = "Data-Graph-Summary";

  public static final String[] FLOWS_NAME   = {
      Analytics.getName(GetClusterGraph.class),
      Analytics.getName(ClusterSubAssembly.class),
      Analytics.getName(GenerateDictionary.class),
      Analytics.getName(GetPropertiesGraph.class),
      Analytics.getName(RelationsGraph.class),
      Analytics.getName(RDFRelationsGraph.class),
      Analytics.getName(RDFMetadataGraph.class)
  };

  private static final Logger  logger       = LoggerFactory.getLogger(DataGraphSummaryCascade.class);

  /*-----------Taps-----------*/
  private final Tap            sindiceSource;

  // Dictionary
  private final Tap            typeDictOut;
  private final Tap            predicateDictOut;
  private final Tap            domainDictOut;
  private final Tap            datatypeDictOut;
  private final Tap            dictionaryTrap;

  private final Tap            getClusterGraphSink;
  private final Tap            getClusterGraphTrap; // the associated trap

  private final Tap            clusterGeneratorGraphSink;

  private final Tap            relationsGraphSink;
  private final Tap            getPropertiesGraphSink;

  // RDF output
  private final Tap            rdfRelationsGraphOut;
  private final Tap            rdfMetadataGraphOut;

  /**
   * Prepare the {@link Tap}s for the {@link Cascade} execution.
   * @param source the input data {@link Tap}
   * @param resultsOut the output folder
   * @param compressDumps if <code>true</code>, the generated RDF in {@link RDFMetadataGraph}
   * and {@link RDFRelationsGraph} are compressed
   */
  private DataGraphSummaryCascade(final Tap source,
                                  final String resultsOut,
                                  final boolean compressDumps) {
    this.sindiceSource = source;

    // Store the dictionaries, for hash to string value mapping
    final Map<String, Fields> dictFields = Analytics.getTailsFields(GenerateDictionary.class);
    final Scheme typeScheme = new HFileScheme(dictFields.get("type"), GenerateDictionary.CLASS_PREFIX);
    typeDictOut = new Hfs(typeScheme, resultsOut + "/type-dictionary", SinkMode.REPLACE);
    final Scheme predicateScheme = new HFileScheme(dictFields.get("predicate"), GenerateDictionary.PREDICATE_PREFIX);
    predicateDictOut = new Hfs(predicateScheme, resultsOut + "/predicate-dictionary", SinkMode.REPLACE);
    final Scheme domainScheme = new HFileScheme(dictFields.get("domain"), GenerateDictionary.DOMAIN_PREFIX);
    domainDictOut = new Hfs(domainScheme, resultsOut + "/domain-dictionary", SinkMode.REPLACE);
    final Scheme datatypeScheme = new HFileScheme(dictFields.get("datatype"), GenerateDictionary.DATATYPE_PREFIX);
    datatypeDictOut = new Hfs(datatypeScheme, resultsOut + "/datatype-dictionary", SinkMode.REPLACE);
    dictionaryTrap = new Hfs(new SequenceFile(Fields.ALL), resultsOut + "/trap-dictionary");

    getClusterGraphSink = new Hfs(new SequenceFile(Analytics.getTailFields(GetClusterGraph.class)),
      resultsOut + "/getClusterGraph");
    getClusterGraphTrap = new Hfs(new SequenceFile(Fields.ALL),
      resultsOut + "/trap-getClusterGraph");
    clusterGeneratorGraphSink = new Hfs(new SequenceFile(
      Analytics.getTailFields(ClusterSubAssembly.class)), resultsOut + "/clusterGeneratorGraph");
    relationsGraphSink = new Hfs(new SequenceFile(
      Analytics.getTailFields(RelationsGraph.class)), resultsOut + "/relationsGraph");
    getPropertiesGraphSink = new Hfs(new SequenceFile(
      Analytics.getTailFields(GetPropertiesGraph.class)), resultsOut + "/getPropertiesGraph");

    final ExtensionTextLine scheme = new ExtensionTextLine("nq", compressDumps ? Compress.ENABLE : Compress.DISABLE);
    rdfRelationsGraphOut = new Hfs(scheme, resultsOut + "/relations-graph-rdf");
    rdfMetadataGraphOut = new Hfs(scheme, resultsOut + "/metadata-graph-rdf");
  }

  /**
   * Run the graph summary {@link Cascade}
   * @param input the path to the input data graph
   * @param inputScheme the {@link Scheme} of the input data
   * @param output the path to the output folder
   * @param flowsProperties set of {@link Properties} for the cascade {@link Flow}s
   * @param clusteringAlgorithm the {@link ClusteringAlgorithm} to apply
   * @return the {@link Cascade}
   * @throws IOException
   */
  public static Cascade run(final String input,
                            final Scheme inputScheme,
                            final String output,
                            final Map<String, Properties> flowsProperties,
                            final ClusteringAlgorithm clusteringAlgorithm)
  throws IOException {
    return run(input, inputScheme, output, flowsProperties, clusteringAlgorithm, true);
  }

  /**
   * Run the graph summary {@link Cascade}
   * @param input the path to the input data graph
   * @param inputScheme the {@link Scheme} of the input data
   * @param output the path to the output folder
   * @param flowsProperties set of {@link Properties} for the cascade {@link Flow}s
   * @param clusteringAlgorithm the {@link ClusteringAlgorithm} to apply
   * @param compressDumps if <code>true</code>, the generated RDF in {@link RDFMetadataGraph}
   * and {@link RDFRelationsGraph} are compressed
   * @return the {@link Cascade}
   * @throws IOException
   */
  public static Cascade run(final String input,
                            final Scheme inputScheme,
                            final String output,
                            final Map<String, Properties> flowsProperties,
                            final ClusteringAlgorithm clusteringAlgorithm,
                            final boolean compressDumps)
  throws IOException {
    return run(input, inputScheme, output, flowsProperties,
      clusteringAlgorithm, compressDumps, null);
  }

  /**
   * Run the graph summary {@link Cascade}
   * @param input the path to the input data graph
   * @param inputScheme the {@link Scheme} of the input data
   * @param output the path to the output folder
   * @param flowsProperties set of {@link Properties} for the cascade {@link Flow}s
   * @param clusteringAlgorithm the {@link ClusteringAlgorithm} to apply
   * @param compressDumps if <code>true</code>, the generated RDF in {@link RDFMetadataGraph}
   * and {@link RDFRelationsGraph} are compressed
   * @param counters the path to the folder for saving this job counters (see {@link AbstractAnalyticsCLI})
   * @return the {@link Cascade}
   * @throws IOException
   */
  public static Cascade run(final String input,
                            final Scheme inputScheme,
                            final String output,
                            final Map<String, Properties> flowsProperties,
                            final ClusteringAlgorithm clusteringAlgorithm,
                            final boolean compressDumps,
                            final File counters)
  throws IOException {
    final Tap source = new Hfs(inputScheme, input);
    final DataGraphSummaryCascade graphCascade = new DataGraphSummaryCascade(source, output, compressDumps);

    // First, create the dictionary
    final Flow<?> dictionaryFlow = graphCascade
      .prepareDictionaryFlow(flowsProperties.get(Analytics.getName(GenerateDictionary.class)));
    dictionaryFlow.complete();
    // Second, compute the graph summary
    final Cascade cascade = graphCascade.getCascade(flowsProperties, clusteringAlgorithm);
    cascade.complete();

    if (counters != null) {
      // Save the Hadoop counters of the dictionary.
      final CountersManager saveCounters = new CountersManager();
      saveCounters.doPipeline(dictionaryFlow, counters);
    }
    return cascade;
  }

  /**
   * Assemble and return the {@link Cascade}
   */
  public Cascade getCascade(final Map<String, Properties> props, final ClusteringAlgorithm clusteringAlgorithm)
  throws IOException {
    final CascadeConnector connector = new CascadeConnector();

    final Flow<?> getClusterGraph = getClusterGraph(props.get(Analytics.getName(GetClusterGraph.class)), clusteringAlgorithm);
    final Flow<?> clusterGeneratorGraph = clusterGeneratorGraph(props.get(Analytics.getName(ClusterSubAssembly.class)), clusteringAlgorithm);

    final Flow<?> relationsGraph = relationsGraph(props.get(Analytics.getName(RelationsGraph.class)));

    final Flow<?> getPropertiesGraph = getPropertiesGraph(props.get(Analytics.getName(GetPropertiesGraph.class)), clusteringAlgorithm);
    final Flow<?> rdfRelationsGraph = prepareRDFRelationsGraphFlow(props.get(Analytics.getName(RDFRelationsGraph.class)));
    final Flow<?> rdfMetadataGraph = prepareRDFMetadataGraphFlow(props.get(Analytics.getName(RDFMetadataGraph.class)));

    return connector.connect(CASCADE_NAME, getClusterGraph, clusterGeneratorGraph, relationsGraph,
      getPropertiesGraph, rdfMetadataGraph, rdfRelationsGraph);
  }

  /**
   * Prepare the {@link GetClusterGraph} flow.
   * @param properties the properties for the flow
   * @param clusteringAlgorithm the {@link ClusteringAlgorithm} to apply
   * @return the {@link Flow}
   * @throws IOException
   */
  private Flow<?> getClusterGraph(final Properties properties, final ClusteringAlgorithm clusteringAlgorithm)
  throws IOException {
    final Map<String, String> conf = GraphSummaryConfig.get(ClusteringAlgorithm.class.getSimpleName(),
      clusteringAlgorithm.toString());
    final PreProcessing preProcessing = PreProcessing.valueOf(conf.get(PreProcessing.class.getSimpleName()));

    logger.info("Using the preprocessing [{}]", preProcessing);
    AppProps.setApplicationJarClass(properties, GetClusterGraph.class);
    final GetClusterGraph pipe = new GetClusterGraph(preProcessing);
    final FlowConnector fc = new HadoopFlowConnector(properties);
    return fc.connect(Analytics.getName(GetClusterGraph.class),
      sindiceSource, getClusterGraphSink, getClusterGraphTrap, pipe);
  }

  /**
   * Prepare the {@link ClusterSubAssembly} flow.
   * @param properties the properties for the flow
   * @param clusteringAlgorithm the {@link ClusteringAlgorithm} to apply
   * @return the {@link Flow}
   * @throws IOException
   */
  private Flow<?> clusterGeneratorGraph(final Properties properties, ClusteringAlgorithm algorithm) {
    logger.info("Using {} clustering", algorithm);

    return ClusteringFactory.launchAlgorithm(algorithm, properties, this.getClusterGraphSink, this.clusterGeneratorGraphSink);
  }

  /**
   * Prepare the {@link RelationsGraph} flow.
   * @param properties the properties for the flow
   * @return the {@link Flow}
   * @throws IOException
   */
  private Flow<?> relationsGraph(final Properties properties){
    AppProps.setApplicationJarClass(properties, RelationsGraph.class);
    final HashMap<String, Tap> sources = new HashMap<String, Tap>();
    sources.put(Analytics.getName(GetClusterGraph.class), this.getClusterGraphSink);
    sources.put(Analytics.getName(ClusterSubAssembly.class), this.clusterGeneratorGraphSink);
    final RelationsGraph pipe = new RelationsGraph();
    return new HadoopFlowConnector(properties).connect(
      Analytics.getName(RelationsGraph.class), sources, this.relationsGraphSink, pipe);
  }

  /**
   * Prepare the {@link GetPropertiesGraph} flow.
   * @param properties the properties for the flow
   * @param clusteringAlgorithm the clustering algorithm that is applied
   * @return the {@link Flow}
   * @throws IOException
   */
  private Flow<?> getPropertiesGraph(final Properties properties, ClusteringAlgorithm clusteringAlgorithm)
  throws IOException {
    final Map<String, String> conf = GraphSummaryConfig.get(ClusteringAlgorithm.class.getSimpleName(),
      clusteringAlgorithm.toString());
    final PropertiesProcessing post = PropertiesProcessing.valueOf(conf.get(PropertiesProcessing.class.getSimpleName()));

    logger.info("Using the PropertiesProcessing [{}]", post);
    AppProps.setApplicationJarClass(properties, GetPropertiesGraph.class);
    final GetPropertiesGraph propertiesGraph = new GetPropertiesGraph(post);
    return new HadoopFlowConnector(properties).connect(Analytics.getName(GetPropertiesGraph.class),
        this.clusterGeneratorGraphSink, this.getPropertiesGraphSink, propertiesGraph);
  }

  /**
   * Prepare the {@link GenerateDictionary} flow.
   * @param properties the properties for the flow
   * @return the {@link Flow}
   * @throws IOException
   */
  private Flow<?> prepareDictionaryFlow(Properties properties) {
    AppProps.setApplicationJarClass(properties, GenerateDictionary.class);

    final GenerateDictionary dict = new GenerateDictionary();
    /*
     * MapFileScheme with ZIP compression Each sink produces 2 outputs: -
     * part-00000: the uncompressed dictionary - part-00000.zip: the zip
     * compressed dictionary
     * 
     * HFileScheme a sink produces a file named dictionary, inside part-XXXXX
     */
    final Map<String, Tap> sources = new HashMap<String, Tap>();
    sources.put(Analytics.getName(dict), sindiceSource);
    final Map<String, Tap> traps = new HashMap<String, Tap>();
    traps.put(Analytics.getName(dict), dictionaryTrap);
    final Map<String, Tap> sinks = new HashMap<String, Tap>();
    sinks.put("type", typeDictOut);
    sinks.put("predicate", predicateDictOut);
    sinks.put("domain", domainDictOut);
    sinks.put("datatype", datatypeDictOut);
    return new HadoopFlowConnector(properties).connect(Analytics.getName(GenerateDictionary.class),
      sources, sinks, traps, dict);
  }

  /**
   * Prepare the {@link RDFRelationsGraph} flow.
   * @param properties the properties for the flow
   * @return the {@link Flow}
   * @throws IOException
   */
  private Flow<?> prepareRDFRelationsGraphFlow(Properties properties) {
    AppProps.setApplicationJarClass(properties, RDFRelationsGraph.class);
    final RDFRelationsGraph rel = new RDFRelationsGraph();

    addDictionariesToCache(properties, typeDictOut.getIdentifier(), predicateDictOut.getIdentifier(),
      domainDictOut.getIdentifier(), datatypeDictOut.getIdentifier());
    // these are used in tests
    try {
      final FileSystem fs = FileSystem.getLocal(new JobConf());
      final Path typePath = new Path(typeDictOut.getIdentifier(), GenerateDictionary.CLASS_PREFIX);
      if (fs.exists(typePath)) {
        properties.setProperty(GenerateDictionary.CLASS_PREFIX, typePath.toUri().toString());
      }
      final Path predicatePath = new Path(predicateDictOut.getIdentifier(), GenerateDictionary.PREDICATE_PREFIX);
      if (fs.exists(predicatePath)) {
        properties.setProperty(GenerateDictionary.PREDICATE_PREFIX, predicatePath.toUri().toString());
      }
      final Path domainPath = new Path(domainDictOut.getIdentifier(), GenerateDictionary.DOMAIN_PREFIX);
      if (fs.exists(domainPath)) {
        properties.setProperty(GenerateDictionary.DOMAIN_PREFIX, domainPath.toUri().toString());
      }
      final Path datatypePath = new Path(datatypeDictOut.getIdentifier(), GenerateDictionary.DATATYPE_PREFIX);
      if (fs.exists(datatypePath)) {
        properties.setProperty(GenerateDictionary.DATATYPE_PREFIX, datatypePath.toUri().toString());
      }
    } catch(IOException e) {
      throw new AnalyticsException(e);
    }

    return new HadoopFlowConnector(properties).connect(
      Analytics.getName(RDFRelationsGraph.class), this.relationsGraphSink,
        rdfRelationsGraphOut, rel);
  }

  /**
   * Prepare the {@link RDFMetadataGraph} flow.
   * @param properties the properties for the flow
   * @return the {@link Flow}
   * @throws IOException
   */
  private Flow<?> prepareRDFMetadataGraphFlow(Properties properties) {
    AppProps.setApplicationJarClass(properties, RDFMetadataGraph.class);
    final RDFMetadataGraph met = new RDFMetadataGraph();

    addDictionariesToCache(properties, typeDictOut.getIdentifier(), predicateDictOut.getIdentifier(),
      domainDictOut.getIdentifier(), datatypeDictOut.getIdentifier());
    // these are used in tests
    try {
      final FileSystem fs = FileSystem.getLocal(new JobConf());
      final Path typePath = new Path(typeDictOut.getIdentifier(), GenerateDictionary.CLASS_PREFIX);
      if (fs.exists(typePath)) {
        properties.setProperty(GenerateDictionary.CLASS_PREFIX, typePath.toUri().toString());
      }
      final Path predicatePath = new Path(predicateDictOut.getIdentifier(), GenerateDictionary.PREDICATE_PREFIX);
      if (fs.exists(predicatePath)) {
        properties.setProperty(GenerateDictionary.PREDICATE_PREFIX, predicatePath.toUri().toString());
      }
      final Path domainPath = new Path(domainDictOut.getIdentifier(), GenerateDictionary.DOMAIN_PREFIX);
      if (fs.exists(domainPath)) {
        properties.setProperty(GenerateDictionary.DOMAIN_PREFIX, domainPath.toUri().toString());
      }
      final Path datatypePath = new Path(datatypeDictOut.getIdentifier(), GenerateDictionary.DATATYPE_PREFIX);
      if (fs.exists(datatypePath)) {
        properties.setProperty(GenerateDictionary.DATATYPE_PREFIX, datatypePath.toUri().toString());
      }
    } catch(IOException e) {
      throw new AnalyticsException(e);
    }

    final FlowConnector rdfMetadata = new HadoopFlowConnector(properties);
    return rdfMetadata.connect(Analytics.getName(RDFMetadataGraph.class),
      getPropertiesGraphSink, rdfMetadataGraphOut, met);
  }

  /**
   * Add the dictionaries {@link HFile} created by {@link GenerateDictionary} to the {@link DistributedCache}.
   * <p>
   * The passed {@link Properties} is updated with the {@link DistributedCache} configuration for the dictionaries.
   * 
   * @param properties the {@link Properties} passed to the {@link FlowConnector}
   * @param type the path to the type dictionary
   * @param predicate the path to the predicate dictionary
   * @param domain the path to the domain dictionary
   * @param datatype the path to the datatype dictionary
   */
  public static void addDictionariesToCache(final Properties properties,
                                            String type,
                                            String predicate,
                                            String domain,
                                            String datatype) {
    final JobConf jobConf = HadoopUtil.createJobConf(properties, new JobConf());

    try {
      logger.info("Adding dictionaries");
      // Type dictionary
      AbstractAnalyticsCLI.addFilesToCache(jobConf, type, GenerateDictionary.CLASS_PREFIX + "*");
      // Predicate dictionary
      AbstractAnalyticsCLI.addFilesToCache(jobConf, predicate, GenerateDictionary.PREDICATE_PREFIX + "*");
      // Domain dictionary
      AbstractAnalyticsCLI.addFilesToCache(jobConf, domain, GenerateDictionary.DOMAIN_PREFIX + "*");
      // Datatype dictionary
      AbstractAnalyticsCLI.addFilesToCache(jobConf, datatype, GenerateDictionary.DATATYPE_PREFIX + "*");
    } catch (Exception e) {
      logger.error("Error adding files to the cache: {}", e);
      throw new AnalyticsException(e);
    }
    properties.putAll(HadoopUtil.createProperties(jobConf));
  }

}
