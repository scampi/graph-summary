/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.sindice.core.analytics.cascading.statistics.CountersManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.hadoop.util.HadoopUtil;
import cascading.management.UnitOfWork;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.TextLine;
import cascading.scheme.hadoop.WritableSequenceFile;
import cascading.stats.CascadingStats;
import cascading.tuple.Fields;
import cascading.tuple.hadoop.TupleSerializationProps;

/**
 * Base class for all Analytics CLIs.
 */
public abstract class AbstractAnalyticsCLI
extends Configured
implements Tool {

  private static final Logger          logger         = LoggerFactory.getLogger(AbstractAnalyticsCLI.class);

  private static final String          HELP           = "help";
  private static final String          CASCADE_CONFIG = "cascade-config";
  protected static final String        INPUT_FORMAT   = "input-format";
  private static final String          COUNTERS       = "counters";
  protected static final String        OUTPUT         = "output";
  protected static final String        INPUT          = "input";

  private OptionParser                 parser;
  protected final CascadeConfYAMLoader cascadeConf;
  private InputFormat                  inputFormat;
  /** List of sources */
  protected List<String>               input          = new ArrayList<String>();
  /** List of sinks */
  protected List<String>               output         = new ArrayList<String>();
  /** The path to the folder with the counters of the executed {@link UnitOfWork} */
  protected File                       counters;

  /**
   * The possible {@link Scheme} of the input files
   */
  public enum InputFormat {
    /** The Cascading {@link SequenceFile} */
    CASCADING_SEQUENCE_FILE,
    /** The Hadoop {@link WritableSequenceFile} */
    HADOOP_SEQUENCE_FILE,
    /** The Cascading {@link TextLine} */
    TEXTLINE
  }

  /**
   * Create a new CLI instance
   */
  public AbstractAnalyticsCLI() {
    cascadeConf = new CascadeConfYAMLoader();
    parser = this.initializeOptionParser();
    this.initializeOptionParser(parser);
  }

  /**
   * This method is to be overwritten by sub-class for setting custom options.
   * @param parser the {@link OptionParser} that will be used on the command line
   */
  protected void initializeOptionParser(OptionParser parser) {
    // do nothing
  }

  /**
   * Initialise the {@link OptionParser} with the generic Analytics options.
   */
  private OptionParser initializeOptionParser() {
    final OptionParser parser = new OptionParser() {
      {
        accepts(HELP, "shows this help message");

        accepts(CASCADE_CONFIG, "YAML-formated file, which is used to pass " +
            "configuration settings to the Cascading Job.")
        .withRequiredArg().ofType(File.class).describedAs("FILE");

        accepts(INPUT_FORMAT, "The format scheme of the input files " +
                              Arrays.toString(InputFormat.values()))
        .withRequiredArg().ofType(InputFormat.class).describedAs("FORMAT")
        .defaultsTo(InputFormat.CASCADING_SEQUENCE_FILE);

        accepts(INPUT, "Input path. When multi-valued, the order is " +
            "important for the executing job.").withRequiredArg().describedAs("DIR")
        .ofType(Path.class).withValuesSeparatedBy(',');

        accepts(OUTPUT, "Output path. When multi-valued, the order is " +
            "important for the executing job.").withRequiredArg()
        .describedAs("DIR").withValuesSeparatedBy(',').ofType(Path.class);

        accepts(COUNTERS, "Path to a folder where the job statistics can be saved to")
        .withRequiredArg().ofType(File.class).describedAs("DIR");
      }
    };
    return parser;
  }

  /**
   * Print a missing option message error and throw a {@link RuntimeException}.
   * @param opt the missing option
   */
  protected void printMissingOptionError(String opt) {
    System.err.println("Error: Missing option " + opt);
    System.err.println("");
    try {
      parser.printHelpOn(System.err);
    } catch (IOException e) {
      logger.error("", e);
    }
    throw new RuntimeException();
  }

  /**
   * Get the {@link Scheme} to be used on the inputs
   * 
   * @param fieldsSelector the {@link Fields} to pass to the {@link Scheme} instance
   * @return the {@link Scheme} instance associated to the {@link #inputFormat}
   */
  @SuppressWarnings("rawtypes")
  protected Scheme getInputScheme(Fields fieldsSelector) {
    switch (inputFormat) {
      case CASCADING_SEQUENCE_FILE:
        return new SequenceFile(fieldsSelector);
      case HADOOP_SEQUENCE_FILE:
        return new WritableSequenceFile(fieldsSelector, Text.class);
      case TEXTLINE:
        return new TextLine(fieldsSelector);
      default:
        throw new EnumConstantNotPresentException(InputFormat.class, inputFormat.toString());
    }
  }

  /**
   * Add the files matched by the pattern to the {@link DistributedCache}.
   * @param jobConf the {@link Configuration} that gets updated with the cached file
   * @param folder the folder to search for the files in
   * @param pattern the pattern to matched the filename to
   * @throws IOException if {@link FileSystem#globStatus(Path)} failed
   */
  public static void addFilesToCache(final Configuration jobConf, String folder, String pattern)
  throws Exception {
    final FileSystem fs = FileSystem.get(jobConf);
    final Path filePattern = new Path(folder, pattern);
    final FileStatus[] cacheFiles = fs.globStatus(filePattern);

    for (FileStatus file : cacheFiles) {
      if (!file.isDir()) {
        final URI uri = file.getPath().toUri();
        DistributedCache.addCacheFile(uri, jobConf);
        logger.info("Added the file [{}] to the DistributedCache", uri);
      }
    }
  }

  /**
   * Add an archive to the {@link DistributedCache}.
   * The archive can then be retrieved using the given linkname.
   * @param properties the {@link Properties} to add the archive to
   * @param archive the archive to add
   * @param linkName the linkname to the archive
   */
  public static void addArchive2DistributedCache(final Properties properties,
                                                 final String archive,
                                                 final String linkName) {
    try {
      final JobConf jobConf = HadoopUtil.createJobConf(properties, new JobConf());

      DistributedCache.createSymlink(jobConf);
      final URI archiveUri = new URI(new Path(archive) + "#" + linkName);
      DistributedCache.addCacheArchive(archiveUri, jobConf);
      properties.putAll(HadoopUtil.createProperties(jobConf));
      logger.info("Added archive to the cache with uri " + archiveUri);
    } catch (URISyntaxException e) {
      logger.error("", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Add a file to the {@link DistributedCache}.
   * The file can then be retrieved using the given linkname.
   * @param properties the {@link Properties} to add the file to
   * @param file the archive to add
   * @param linkName the linkname to the file
   */
  public static void addFile2DistributedCache(final Properties properties,
                                              final String file,
                                              final String linkName) {
    try {
      final JobConf jobConf = HadoopUtil.createJobConf(properties, new JobConf());

      DistributedCache.createSymlink(jobConf);
      final URI fileUri = new URI(new Path(file) + "#" + linkName);
      DistributedCache.addCacheFile(fileUri, jobConf);
      properties.putAll(HadoopUtil.createProperties(jobConf));
      logger.info("Added file to the cache with uri " + fileUri);
    } catch (URISyntaxException e) {
      logger.error("", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public int run(String[] args)
  throws Exception {
    OptionSet options = null;

    try {
      options = parser.parse(args);
    } catch (final OptionException e) {
      logger.error("", e);
      parser.printHelpOn(System.err);
      System.exit(1);
    }

    if (options.has(HELP)) {
      parser.printHelpOn(System.out);
      System.exit(0);
    }

    // INPUT_FORMAT
    inputFormat = (InputFormat) options.valueOf(INPUT_FORMAT);
    // INPUT
    if (options.has(INPUT)) {
      final List<Path> inputs = ((List<Path>) options.valuesOf(INPUT));
      for (Path path: inputs) {
        input.add(path.toUri().toString());
      }
    }
    // OUTPUT
    if (options.has(OUTPUT)) {
      final List<Path> outputs = ((List<Path>) options.valuesOf(OUTPUT));
      for (Path path: outputs) {
        output.add(path.toUri().toString());
      }
    }
    // CASCADE_CONFIG
    if (options.has(CASCADE_CONFIG)) {
      cascadeConf.load(getConf(), new FileInputStream((File) options.valueOf(CASCADE_CONFIG)));
    } else {
      cascadeConf.load(getConf(), null);
    }
    // Add custom serializations
    SerializationsLoader.load(cascadeConf.defaultProperties);
    // TupleSerializationProps is loaded with the serialization classes
    if (cascadeConf.defaultProperties.containsKey(TupleSerializationProps.HADOOP_IO_SERIALIZATIONS)) {
      getConf().set(TupleSerializationProps.HADOOP_IO_SERIALIZATIONS,
        cascadeConf.defaultProperties.getProperty(TupleSerializationProps.HADOOP_IO_SERIALIZATIONS));
    }

    // COUNTERS
    counters = (File) options.valueOf(COUNTERS);

    // Execute the job
    try {
      final UnitOfWork<? extends CascadingStats> work = doRun(options);
      if (options.has(COUNTERS)) {
        counters.mkdirs();
        // Save the Job configuration
        saveJobConfiguration(counters, args, cascadeConf.getCascadeProperties());
        // Save the counters
        if (work != null) {
          final CountersManager counters = new CountersManager();
          counters.doPipeline(work, this.counters);
        } else {
          logger.warn("Unable to store the counters, the unit of work is null");
        }
      }
      return work == null || work.getStats().isSuccessful() ? 0 : 1;
    } catch(Exception e) {
      GenericOptionsParser.printGenericCommandUsage(System.err);
      System.err.println("Tool options are:");
      parser.printHelpOn(System.err);
      throw new RuntimeException(e);
    }
  }

  /**
   * Save this Hadoop job configuration into a file named "configuration.bak".
   * @param path the path to the folder where to write the backup file
   * @param args the arguments given to the subclassed CLI
   * @param properties the properties given to this Hadoop job
   * @throws IOException if an error occurs while writing the job configuration
   */
  private void saveJobConfiguration(final File path, final String[] args, final Map<String, Properties> properties)
  throws IOException {
    final BufferedWriter w = new BufferedWriter(new FileWriter(new File(path, "configuration.bak")));

    try {
      // The arguments
      w.write("# The job arguments\n\n\t");
      for (String arg : args) {
        w.append(arg).append(' ');
      }
      // The parameters
      final Map<Object, Object> sort = new TreeMap<Object, Object>();
      w.append("\n\n# The job parameters\n\n");
      for (Entry<String, Properties> props : properties.entrySet()) {
        w.append("## FLow [").append(props.getKey()).append("]\n");

        sort.clear();
        sort.putAll(props.getValue());
        for (Entry<Object, Object> pv : sort.entrySet()) {
          w.append('\t').append(String.valueOf(pv.getKey())).append(" = ")
           .append(String.valueOf(pv.getValue())).append('\n');
        }
        w.append('\n');
      }
    } finally {
      w.close();
    }
  }

  /**
   * Execute the job with given specific options
   * @param options the {@link OptionSet} of the CLI subclass
   * @return exit code
   * @throws Exception
   */
  protected abstract UnitOfWork<? extends CascadingStats> doRun(OptionSet options) throws Exception;

}
