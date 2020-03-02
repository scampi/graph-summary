/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading.ecluster;

import java.util.Properties;

import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.riffle.RiffleProcessFlow;
import org.sindice.graphsummary.cascading.ecluster.datatype.DatatypeClusterSubAssembly;
import org.sindice.graphsummary.cascading.ecluster.fbisimulation.FBisimulationProcess;
import org.sindice.graphsummary.cascading.ecluster.ioproperties.IOPropertiesClusterGeneratorSubAssembly;
import org.sindice.graphsummary.cascading.ecluster.namespace.NSClusterSubAssembly;
import org.sindice.graphsummary.cascading.ecluster.properties.PropertiesClusterSubAssembly;
import org.sindice.graphsummary.cascading.ecluster.singletype.SingleTypeClusterSubAssembly;
import org.sindice.graphsummary.cascading.ecluster.types.TypesClusterSubAssembly;
import org.sindice.graphsummary.cascading.ecluster.typesproperties.TypesPropertiesClusterSubAssembly;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.property.AppProps;
import cascading.tap.Tap;

/**
 * This function will call the correct algorithm to compute the Cluster-ID
 * 
 * @author Pierre Bailly-Ferry <pierre.bailly@deri.org>
 */
public class ClusteringFactory {

  /**
   * The list of possible clustering algorithms
   */
  public enum ClusteringAlgorithm {
    TYPES, PROPERTIES, TYPES_PROPERTIES, SINGLE_TYPE, IO_PROPERTIES, IO_PROPERTIES_CLASSES, F_BISIMULATION, NAMESPACE,
    IO_NAMESPACE, DATATYPE
  }

  private ClusteringFactory() {}

  public static Flow launchAlgorithm(final ClusteringAlgorithm cluster,
                                     final Properties properties,
                                     final Tap sourceTap,
                                     final Tap sinkTap) {
    final String name = Analytics.getName(ClusterSubAssembly.class);
    final FlowConnector fc = new HadoopFlowConnector(properties);

    switch (cluster) {
      case TYPES:
        AppProps.setApplicationJarClass(properties, TypesClusterSubAssembly.class);
        return fc.connect(name, sourceTap, sinkTap, new TypesClusterSubAssembly());
      case PROPERTIES:
        AppProps.setApplicationJarClass(properties, PropertiesClusterSubAssembly.class);
        return fc.connect(name, sourceTap, sinkTap, new PropertiesClusterSubAssembly());
      case TYPES_PROPERTIES:
        AppProps.setApplicationJarClass(properties, TypesPropertiesClusterSubAssembly.class);
        return fc.connect(name, sourceTap, sinkTap, new TypesPropertiesClusterSubAssembly());
      case SINGLE_TYPE:
        AppProps.setApplicationJarClass(properties, SingleTypeClusterSubAssembly.class);
        return fc.connect(name, sourceTap, sinkTap, new SingleTypeClusterSubAssembly());
      case IO_PROPERTIES:
        AppProps.setApplicationJarClass(properties, IOPropertiesClusterGeneratorSubAssembly.class);
        return fc.connect(name, sourceTap, sinkTap, new IOPropertiesClusterGeneratorSubAssembly(false));
      case IO_PROPERTIES_CLASSES:
        AppProps.setApplicationJarClass(properties, IOPropertiesClusterGeneratorSubAssembly.class);
        return fc.connect(name, sourceTap, sinkTap, new IOPropertiesClusterGeneratorSubAssembly(true));
      case F_BISIMULATION:
        AppProps.setApplicationJarClass(properties, FBisimulationProcess.class);
        final FBisimulationProcess process = new FBisimulationProcess(sourceTap, sinkTap, properties);
        return new RiffleProcessFlow<FBisimulationProcess>(Analytics.getName(ClusterSubAssembly.class), process);
      case NAMESPACE:
        AppProps.setApplicationJarClass(properties, NSClusterSubAssembly.class);
        return fc.connect(name, sourceTap, sinkTap, new NSClusterSubAssembly(false));
      case IO_NAMESPACE:
        AppProps.setApplicationJarClass(properties, NSClusterSubAssembly.class);
        return fc.connect(name, sourceTap, sinkTap, new NSClusterSubAssembly(true));
      case DATATYPE:
        AppProps.setApplicationJarClass(properties, DatatypeClusterSubAssembly.class);
        return fc.connect(name, sourceTap, sinkTap, new DatatypeClusterSubAssembly());
      default:
        throw new EnumConstantNotPresentException(ClusteringFactory.ClusteringAlgorithm.class, cluster.toString());
    }
  }

}
