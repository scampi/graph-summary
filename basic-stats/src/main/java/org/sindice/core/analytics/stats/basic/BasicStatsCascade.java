package org.sindice.core.analytics.stats.basic;

import java.util.Map;
import java.util.Properties;

import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.scheme.ExtensionTextLine;
import org.sindice.core.analytics.stats.basic.assembly.BasicClassStats;
import org.sindice.core.analytics.stats.basic.assembly.BasicFormatStats;
import org.sindice.core.analytics.stats.basic.assembly.BasicNamespaceStats;
import org.sindice.core.analytics.stats.basic.assembly.BasicPredicateStats;
import org.sindice.core.analytics.stats.basic.assembly.BasicUriStats;
import org.sindice.core.analytics.stats.basic.assembly.SortAndFilter;
import org.sindice.core.analytics.stats.basic.rdf.RDFCollectionClassStats;
import org.sindice.core.analytics.stats.basic.rdf.RDFCollectionFormatStats;
import org.sindice.core.analytics.stats.basic.rdf.RDFCollectionNamespaceStats;
import org.sindice.core.analytics.stats.basic.rdf.RDFCollectionPredicateStats;
import org.sindice.core.analytics.stats.basic.rdf.RDFCollectionUriStats;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.TextLine;
import cascading.scheme.hadoop.TextLine.Compress;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class BasicStatsCascade {

  public static final String             TOPK             = "topk";
  public static final String             FILTER_THRESHOLD = "filter-threshold";
  public static final String             TYPE             = "type";
  public static final String             SORT_FIELDS      = "sort-fields";
  public static final String             FILTER_FIELDS    = "filter-fields";
  public static final String             DESCEND_SORT     = "descend-sort";

  public static final String             STAT_FLOW        = "stat";
  public static final String             TOPK_FLOW        = "topk";
  public static final String             RDF_FLOW         = "rdf";
  public static final String[]           FLOWS_NAME        = {
    STAT_FLOW,
    TOPK_FLOW,
    RDF_FLOW
  };

  private static long                    topN             = 100000;
  private static int                     filterThreshold  = 5;
  private static Class<? extends Number> type             = Integer.class;
  private static String                  sortFields       = null;
  private static String                  filterField      = null;
  private static boolean                 descendSort      = true;

  private BasicStatsCascade() {}

  public static Cascade getClassCascade(final String input,
                                        final Scheme sourceScheme,
                                        final String output,
                                        final Map<String, Properties> properties) {
    final BasicClassStats stats = new BasicClassStats();
    final RDFCollectionClassStats rdfStats = new RDFCollectionClassStats();
    return getCascade(input, sourceScheme, output, stats, rdfStats, properties);
  }

  public static Cascade getPredicateCascade(final String input,
                                            final Scheme sourceScheme,
                                            final String output,
                                            final Map<String, Properties> properties) {
    final BasicPredicateStats stats = new BasicPredicateStats();
    final RDFCollectionPredicateStats rdfStats = new RDFCollectionPredicateStats();
    return getCascade(input, sourceScheme, output, stats, rdfStats, properties);
  }

  public static Cascade getFormatCascade(final String input,
                                         final Scheme sourceScheme,
                                         final String output,
                                         final Map<String, Properties> properties) {
    final BasicFormatStats stats = new BasicFormatStats();
    final RDFCollectionFormatStats rdfStats = new RDFCollectionFormatStats();
    return getCascade(input, sourceScheme, output, stats, rdfStats, properties);
  }

  public static Cascade getUriCascade(final String input,
                                      final Scheme sourceScheme,
                                      final String output,
                                      final Map<String, Properties> properties) {
    final BasicUriStats stats = new BasicUriStats();
    final RDFCollectionUriStats rdfStats = new RDFCollectionUriStats();
    return getCascade(input, sourceScheme, output, stats, rdfStats, properties);
  }

  public static Cascade getNamespaceCascade(final String input,
                                            final Scheme sourceScheme,
                                            final String output,
                                            final Map<String, Properties> properties) {
    final BasicNamespaceStats stats = new BasicNamespaceStats();
    final RDFCollectionNamespaceStats rdfStats = new RDFCollectionNamespaceStats();
    return getCascade(input, sourceScheme, output, stats, rdfStats, properties);
  }

  private static Cascade getCascade(final String input,
                                    final Scheme sourceScheme,
                                    final String output,
                                    final AnalyticsSubAssembly stats,
                                    final AnalyticsSubAssembly rdfStats,
                                    final Map<String, Properties> properties) {
    final Tap statsSource = new Hfs(sourceScheme, input);
    final Tap statsSink = new Hfs(new SequenceFile(Analytics.getTailFields(stats)),
      output + "/MAP", SinkMode.REPLACE);

    // Setup the Stats flow
    AppProps.setApplicationJarClass(properties.get(STAT_FLOW), BasicStatsCascade.class);
    final Flow<?> statsFlow = new HadoopFlowConnector(properties.get(STAT_FLOW))
    .connect(Analytics.getName(stats), statsSource, statsSink, stats);

    // Setup the TopK flow
    settings(Analytics.getTailFields(stats), properties.get(TOPK_FLOW));
    final Tap topkSink = new Hfs(new TextLine(),
      output + "/TEXT", SinkMode.REPLACE);
    SortAndFilter
    .settings(topN, filterThreshold, type, null, sortFields, filterField, descendSort);
    SortAndFilter.setFieldDeclaration(Analytics.getTailFields(stats));
    SortAndFilter pipe = new SortAndFilter("sort-n-filter");
    final Flow<?> topKFlow = new HadoopFlowConnector(properties.get(TOPK_FLOW))
      .connect("sort-n-filter", statsSink, topkSink, pipe);

    // Setup the RDF export flow
    final Properties rdfProperties = properties.get(RDF_FLOW);
    final Tap rdfSink;
    if (Boolean.parseBoolean(rdfProperties.getProperty("mapred.output.compress", "false"))) {
      rdfSink = new Hfs(new ExtensionTextLine("nq", Compress.ENABLE),
        output + "/RDF", SinkMode.REPLACE);
    } else {
      rdfSink = new Hfs(new ExtensionTextLine("nq"), output + "/RDF", SinkMode.REPLACE);
    }
    final Flow<?> rdfFlow = new HadoopFlowConnector(rdfProperties)
    .connect(Analytics.getName(rdfStats), statsSink, rdfSink, rdfStats);

    final Cascade cascade = new CascadeConnector(properties.get(RDF_FLOW))
    .connect(statsFlow, topKFlow, rdfFlow);
    return cascade;
  }

  private static void settings(Fields fields, Properties properties) {
    // TOPK
    if (properties.containsKey(TOPK)) {
      topN = Long.valueOf(properties.getProperty(TOPK));
      if (topN < 1) {
        topN = Long.MAX_VALUE;
      }
    }
    // FILTER_THRESHOLD
    if (properties.containsKey(FILTER_THRESHOLD)) {
      filterThreshold = Integer.valueOf(properties
      .getProperty(FILTER_THRESHOLD));
    }
    // TYPE
    if (properties.containsKey(TYPE)) {
      // Set type of data that will be sorted
      type = Integer.class;
      final String s = properties.getProperty(TYPE);
      if (s.equalsIgnoreCase("long")) {
        type = Long.class;
      } else if (s.equalsIgnoreCase("int")) {
        type = Integer.class;
      } else {
        throw new IllegalArgumentException("Invalid Type");
      }
    }
    // FILTER_FIELDS
    if (properties.containsKey(FILTER_FIELDS)) {
      final int n = Integer.valueOf(properties.getProperty(FILTER_FIELDS));
      if (n < 1 || n > fields.size()) {
        throw new IllegalArgumentException("filter field is invalid");
      }
      filterField = fields.get(n - 1).toString();
    } else {
      throw new IllegalArgumentException("Missing configuration parameter: " +
                                         FILTER_FIELDS);
    }
    // SORT_FIELDS
    if (properties.containsKey(SORT_FIELDS)) {
      final int n = Integer.valueOf(properties.getProperty(SORT_FIELDS));
      if (n < 1 || n > fields.size()) {
        throw new IllegalArgumentException("Sort field is invalid: " + n);
      }
      sortFields = fields.get(n - 1).toString();
    } else {
      throw new IllegalArgumentException("Missing configuration parameter: " +
                                         SORT_FIELDS);
    }
    // DESCEND_SORT
    if (properties.containsKey(DESCEND_SORT)) {
      descendSort = Boolean.parseBoolean(properties.getProperty(DESCEND_SORT));
    }
  }

}
