/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.testHelper.iotransformation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.junit.Assert;
import org.sindice.core.analytics.cascading.AnalyticsParameters;
import org.sindice.core.analytics.cascading.AnalyticsSubAssembly;
import org.sindice.core.analytics.cascading.annotation.Analytics;
import org.sindice.core.analytics.cascading.dictionary.HFileDictionary;
import org.sindice.core.analytics.cascading.riffle.RiffleProcessFlow;
import org.sindice.core.analytics.cascading.scheme.HFileScheme;
import org.sindice.core.analytics.testHelper.CheckScheme;
import org.sindice.core.analytics.util.AnalyticsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import riffle.process.Process;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.TextDelimited;
import cascading.stats.FlowStats;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Comparison;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

/**
 * Implements a new set of method to execute the tests using human readable
 * input and output fields. Creation of a new Object initialises the source and
 * sink tap, the files have to be in TSV format. Provided output and input files
 * and formats you can run a user specified test and ensure that the test has
 * successfully been validated
 * 
 * <p>
 * 
 * The {@link Tap} instances for the input files are created using a
 * {@link Scheme} based on the file's extension:
 * <ul>
 * <li>{@link CheckScheme} for <b>*.check</b>;</li>
 * <li>{@link TextDelimited} for <b>*.*</b>.</li>
 * </ul>
 * 
 * The {@link CheckScheme} should be used instead of the {@link TextDelimited},
 * due to problems to the handling of empty/null fields, and to be able to write
 * the content of a field over multiple lines.
 */
@SuppressWarnings("rawtypes")
public class UnitOfWorkTestHelper {

  private static final Map<String, Class<?>[]> typesSources  = new HashMap<String, Class<?>[]>();
  private static final Map<String, String>     testSources   = new HashMap<String, String>();
  private static final Map<String, Class<?>[]> typesSinks    = new HashMap<String, Class<?>[]>();
  private static final Map<String, String>     expectedSinks = new HashMap<String, String>();

  // This contains the path to the folders where the assembly output are written to
  private static final Map<String, Tap>        actualSinks   = new HashMap<String, Tap>();

  private static final Logger                  logger        = LoggerFactory.getLogger(UnitOfWorkTestHelper.class);

  private UnitOfWorkTestHelper() {}

  /**
   * Initialise the test definition
   */
  public static void reset() {
    typesSources.clear();
    testSources.clear();
    typesSinks.clear();
    expectedSinks.clear();
    actualSinks.clear();
  }

  /**
   * Returns the paths to the folders where the assembly outputs are written to.
   * The key of the {@link Map} is the the name of the associated tail.
   */
  public static Map<String, Tap> getSinks() {
    return actualSinks;
  }

  /**
   * Specify the conversion types for each part of the Tuples contained in the
   * input file
   * 
   * @param name the name of the head of the tested {@link AnalyticsSubAssembly} the data goes to
   * @param source the file that contains the data, with the format defined by types.
   * @param types the format description of the data file
   */
  public static void addSource(final String name,
                               final String source,
                               final Class<?>[] types) {
    if (testSources.containsKey(name) || typesSources.containsKey(name)) {
      throw new IllegalArgumentException("The source named " + name + " has" +
          " already been set.");
    }
    testSources.put(name, source);
    typesSources.put(name, types);
    if (!FileUtils.getFile(source).exists()) {
      throw new IllegalArgumentException("The file " + source + " does not exist");
    }
  }

  /**
   * Specify the conversion types for each part of the Tuples contained in the
   * output file
   * 
   * @param name the name of the tail of the tested {@link AnalyticsSubAssembly} the data is expected from
   * @param sink the file that contains the data, with the format defined by types.
   * @param types the format description of the data file
   */
  public static void addSink(final String name,
                             final String sink,
                             final Class<?>[] types) {
    if (expectedSinks.containsKey(name) || typesSinks.containsKey(name)) {
      throw new IllegalArgumentException("The sink named " + name + " has" +
          " already been set.");
    }
    expectedSinks.put(name, sink);
    typesSinks.put(name, types);
    if (!FileUtils.getFile(sink).exists()) {
      throw new IllegalArgumentException("The file " + sink + " does not exist");
    }
  }

  /**
   * This method executes this assembly on the test inputs files, and asserts
   * that the output data is what was expected in the test outputs files.
   * @param assembly The {@link AnalyticsSubAssembly} or Riffle {@link Process} to test
   * @param testFolder the path to the temporary test folder
   * @param flowProperties the set of {@link Properties} to pass to the assembly
   * @param assemblyArgs a list of arguments to pass to the assembly
   */
  public static void runTestAssembly(final Class<?> assembly,
                                     final File testFolder,
                                     final Properties flowProperties,
                                     final Object... assemblyArgs)
  throws Exception {
    runTestAssembly(assembly, testFolder, flowProperties, (Map<String, Map<String, Long>>) null, assemblyArgs);
  }

  /**
   * Pre-process the test data, applying the {@link ConvertFunction} on the fields.
   * @param assembly The {@link AnalyticsSubAssembly} to test
   * @param testFolder the path to the temporary test folder
   * @param sources the list of source {@link Tap}s
   * @param inPipes the list of head pipes, with the added {@link ConvertFunction}
   * @return A {@link List} of {@link Pipe}s, corresponding to the heads of the tested assembly
   */
  private static void preProcess(final Class assembly,
                                 final File testFolder, List<Pipe> inPipes,
                                 final Map<String, Tap> sources) {
    for (final Entry<String, Fields> tailFields : Analytics.getTailsFields(assembly).entrySet()) {
      final String pathSink = new File(testFolder, "actualSink" + tailFields.getKey()
        + Math.random()).getAbsolutePath();
      // With SequenceFile, we can test the custom Serialization classes
      actualSinks.put(tailFields.getKey(), new Hfs(new SequenceFile(tailFields.getValue()), pathSink, SinkMode.REPLACE));
    }
    for (final Entry<String, Fields> headFields : Analytics.getHeadsFields(assembly).entrySet()) {
      if (!typesSources.containsKey(headFields.getKey())) {
        throw new IllegalArgumentException("Missing input test file for the head " + headFields.getKey());
      }
      // Parse the input file to extract the tuples and convert them into the specified format
      final String name = headFields.getKey();
      final Fields fields = headFields.getValue();
      sources.put(name, getTap(testSources.get(name), fields));
      inPipes.add(new Each(name, new ConvertFunction(fields, typesSources.get(name))));
    }
  }

  /**
   * Post-process the data from the tested {@link SubAssembly}.
   * @param assembly the class to test
   * @param pipes the current tails of the tested class
   * @return a {@link List} of {@link Pipe}s, with {@link Function}s added for post-processing
   */
  private static List<Pipe> postProcess(final Class assembly,
                                        final Pipe[] pipes) {
    final List<Pipe> outPipes = new ArrayList<Pipe>();
    final Map<String, Fields> tails = Analytics.getTailsFields(assembly);

    if (tails.size() != pipes.length) {
      throw new IllegalArgumentException("Expected " + tails + ", but got " + Arrays.toString(pipes));
    }
    // sort the fields, if needed
    int i = 0;
    for (Entry<String, Fields> entry : tails.entrySet()) {
      final String name = entry.getKey();
      final Fields fields = entry.getValue();
      outPipes.add(new Each(pipes[i++], new SortFunction(fields, typesSinks.get(name))));
    }
    return outPipes;
  }

  /**
   * This method executes this assembly on the test inputs files, and asserts
   * that the output data is what was expected in the test outputs files.
   * @param assembly The {@link AnalyticsSubAssembly} or Riffle {@link Process} to test
   * @param testFolder the path to the temporary test folder
   * @param flowProperties the set of {@link Properties} to pass to the assembly
   * @param counters the expected counters
   * @param assemblyArgs a list of arguments to pass to the assembly
   */
  public static void runTestAssembly(final Class<?> assembly,
                                     final File testFolder,
                                     final Properties flowProperties,
                                     final Map<String, Map<String, Long>> counters,
                                     final Object... assemblyArgs)
  throws Exception {
    final FlowConnector flowConnector = new HadoopFlowConnector(flowProperties);

    if (!AnalyticsSubAssembly.class.isAssignableFrom(assembly) && (assembly.getAnnotation(Process.class)) == null) {
      throw new IllegalArgumentException("The tested class is neither an AnalyticsSubAssembly nor a Riffle process.");
    }
    if (testSources.isEmpty() || expectedSinks.isEmpty()) {
      throw new IllegalArgumentException("Test resources must be specified using #addSource and #addSink");
    }

    /*
     * Test input
     */
    final List<Pipe> inPipes = new ArrayList<Pipe>();
    final Map<String, Tap> sources = new HashMap<String, Tap>();
    preProcess(assembly, testFolder, inPipes, sources);

    final Flow<?> flowInput;
    if (AnalyticsSubAssembly.class.isAssignableFrom(assembly)) {
      flowInput = getAssemblyFlow(flowConnector, assembly, sources, inPipes, assemblyArgs);
      assertCounters(counters, flowInput);
    } else {
      flowInput = getRiffleFlow(flowConnector, testFolder, flowProperties, counters, assembly, sources, inPipes);
    }

    /*
     * Expected test output
     */
    final Map<String, Tap> expectedSeqSinks = new HashMap<String, Tap>();
    final List<Pipe> expectedOutPipes = new ArrayList<Pipe>();
    final Map<String, Tap> sinks = new HashMap<String, Tap>();
    prepareExpectedOutput(expectedSeqSinks, expectedOutPipes, sinks, assembly, testFolder);
    // Connect the flow to convert the output field to the same format as the
    // output of the test in order to compare
    final Flow<?> flowOutput = flowConnector.connect(sinks, expectedSeqSinks,
      expectedOutPipes.toArray(new Pipe[expectedOutPipes.size()]));
    flowOutput.complete();

    //Compare the expected results and the results of the test
    try {
      for (final String tailName : Analytics.getTailsFields(assembly).keySet()) {
        TupleEntryIterator iteratorInput = flowInput.openSink(tailName);
        TupleEntryIterator iteratorOutput = flowOutput.openSink(tailName);
        assertTupleEquals((Configuration) flowInput.getConfig(), tailName, iteratorInput, iteratorOutput);
      }
    } catch (IOException e) {
      logger.error("Assertion has failed or failure to open the sinks", e);
    }
  }

  /**
   * Runs the {@link Process}.
   * @param flowConnector the {@link FlowConnector}
   * @param testFolder the path to the test folder
   * @param properties the {@link Properties} for the process
   * @param counters the expected counters values
   * @param assembly the process class
   * @param sources the source {@link Map}
   * @param inPipes the heads of the process
   * @return the {@link Flow} of this process
   */
  private static Flow getRiffleFlow(final FlowConnector flowConnector,
                                    final File testFolder,
                                    final Properties properties,
                                    final Map<String, Map<String, Long>> counters,
                                    final Class<?> assembly,
                                    final Map<String, Tap> sources,
                                    final List<Pipe> inPipes) {
    final Map<String, Tap> riffleSinks = new HashMap<String, Tap>();
    final List<Pipe> rifflePipes = new ArrayList<Pipe>();

    // Intermediate sinks
    for (final Entry<String, Fields> tailFields : Analytics.getTailsFields(assembly).entrySet()) {
      final String name = tailFields.getKey();
      final String sink = new File(testFolder, "riffleSink" + name + Math.random()).getAbsolutePath();
      // With SequenceFile, we can test the custom Serialization classes
      riffleSinks.put(name, new Hfs(new SequenceFile(tailFields.getValue()), sink));
      rifflePipes.add(new Pipe(name));
    }

    // Get the flow
    final Flow riffleFlow;
    try {
      final Constructor<?> c = assembly.getConstructor(Tap[].class, Tap[].class, Pipe[].class, Properties.class);
      Object process = c.newInstance(sources.values().toArray(new Tap[0]), riffleSinks.values().toArray(new Tap[0]),
        inPipes.toArray(new Pipe[0]), properties);
      riffleFlow = new RiffleProcessFlow(Analytics.getName(assembly), process);
    } catch (Exception e) {
      throw new RuntimeException("Unable to create instance", e);
    }
    riffleFlow.complete();
    assertCounters(counters, riffleFlow);
    riffleFlow.cleanup();

    // Sort the data
    final List<Pipe> outPipes = postProcess(assembly, rifflePipes.toArray(new Pipe[0]));
    final Flow flow = flowConnector.connect(riffleSinks, actualSinks, outPipes.toArray(new Pipe[0]));
    flow.complete();
    return flow;
  }

  /**
   * Runs the {@link AnalyticsSubAssembly}.
   * @param flowConnector the {@link FlowConnector}
   * @param assembly the {@link AnalyticsSubAssembly} class
   * @param sources the sources {@link Map}
   * @param inPipes the heads of the assembly
   * @param assemblyArgs the arguments for the assembly
   * @return the {@link Flow} that executed the assembly
   */
  private static Flow getAssemblyFlow(final FlowConnector flowConnector,
                                      final Class<?> assembly,
                                      final Map<String, Tap> sources,
                                      final List<Pipe> inPipes,
                                      final Object... assemblyArgs) {
    // Creating a new instance of the constructor in the class to test and passing the pipe as an argument
    final SubAssembly pipe;

    try {
      if (assemblyArgs != null && assemblyArgs.length != 0) {
        final Constructor<?> constructor = assembly.getConstructor(Pipe[].class, Object[].class);
        pipe = (SubAssembly) constructor.newInstance(
          (Object) inPipes.toArray(new Pipe[inPipes.size()]),
          assemblyArgs);
      } else {
        final Constructor<?> constructor = assembly.getConstructor(Pipe[].class);
        pipe = (SubAssembly) constructor.newInstance(
          (Object) inPipes.toArray(new Pipe[inPipes.size()]));
      }
    } catch (Exception e) {
      throw new RuntimeException("Unable to create instance", e);
    }
    final List<Pipe> outPipes = postProcess(assembly, pipe.getTails());
    final Flow flow = flowConnector.connect(sources, actualSinks, outPipes.toArray(new Pipe[0]));
    flow.complete();
    return flow;
  }

  /**
   * This method prepares the expected data outputed by the tested assembly
   * so that it can be compared against {@link #assertTupleEquals(String, TupleEntryIterator, TupleEntryIterator)}.
   * @param expectedSeqSinks the expected data sources
   * @param expectedOutPipes the pre-processed tails
   * @param sinks the sink taps
   * @param assembly The {@link AnalyticsSubAssembly} or Riffle {@link Process} to test
   * @param testFolder the path to the temporary test folder
   */
  private static void prepareExpectedOutput(final Map<String, Tap> expectedSeqSinks,
                                            final List<Pipe> expectedOutPipes,
                                            final Map<String, Tap> sinks,
                                            final Class assembly,
                                            final File testFolder) {
    for (final Entry<String, Fields> tailFields : Analytics.getTailsFields(assembly).entrySet()) {
      final String pathSink = new File(testFolder, "expectedSink" + tailFields.getKey()
        + Math.random()).getAbsolutePath();
      // With SequenceFile, we can test the custom Serialization classes
      expectedSeqSinks.put(tailFields.getKey(), new Hfs(new SequenceFile(tailFields.getValue()), pathSink, SinkMode.REPLACE));
    }
    for (final Entry<String, Fields> tailFields : Analytics.getTailsFields(assembly).entrySet()) {
      if (!typesSinks.containsKey(tailFields.getKey())) {
        throw new IllegalArgumentException("Missing input test file for the tail " + tailFields.getKey());
      }
      // Parse the output file to extract the tuples and convert them into the
      // specified formats
      final String name = tailFields.getKey();
      final Fields fields = tailFields.getValue();
      sinks.put(name, getTap(expectedSinks.get(name), fields));
      Pipe postPipe = new Each(name, new ConvertFunction(fields, typesSinks.get(name)));
      postPipe = new Each(postPipe, new SortFunction(fields, typesSinks.get(name)));
      expectedOutPipes.add(postPipe);
    }
  }

  /**
   * Asserts that this {@link Flow} contains the expected counters values
   * @param counters the expected set of counters
   * @param flow the {@link Flow} to get counters from
   */
  public static void assertCounters(final Map<String, Map<String, Long>> counters,
                                    final Flow<?> flow) {
    if (counters == null) {
      return;
    }
    final FlowStats stats = flow.getStats();
    for (final Entry<String, Map<String, Long>> group : counters.entrySet()) {
      for (final Entry<String, Long> counter : group.getValue().entrySet()) {
        final long expected = counter.getValue().longValue();
        final long actual = stats.getCounterValue(group.getKey(), counter.getKey());
        assertEquals("group=" + group + " counter=" + counter, expected, actual);
      }
    }
  }

  /**
   * Returns a {@link Tap} instance for the given file, using a {@link Scheme}
   * based on the file's extension:
   * <ul>
   * <li>{@link CheckScheme} for <b>*.check</b>;</li>
   * <li>{@link TextDelimited} for <b>*.*</b>.</li>
   * </ul>
   * 
   * @param filename the name of the test file
   * @param fields the fields described in the file
   * @return a {@link Tap} instance
   */
  private static Tap getTap(final String filename,
                            final Fields fields) {
    if (filename.endsWith(".check")) {
      return new Hfs(new CheckScheme(fields), filename);
    }
    return new Hfs(new TextDelimited(fields, false, "\t"), filename);
  }

  /**
   * The purpose of this method is to assert the equality of the content
   * (Tuples) of the converted input and output files.
   * @param conf the {@link Configuration} of the job
   * @param tailName the name of the sink
   * @param actualIterator {@link TupleEntryIterator} over the actual data
   * @param expectedIterator {@link TupleEntryIterator} over the expected data
   */
  public static void assertTupleEquals(final Configuration conf,
                                       final String tailName,
                                       final TupleEntryIterator actualIterator,
                                       final TupleEntryIterator expectedIterator) {
    final List<Tuple> actualTuples = new ArrayList<Tuple>();
    final List<Tuple> expectedTuples = new ArrayList<Tuple>();
    final SerializationFactory sf = new SerializationFactory(conf);

    while (actualIterator.hasNext()) {
      TupleEntry tupleEntry = (TupleEntry) actualIterator.next();
      actualTuples.add(tupleEntry.getTupleCopy());
    }
    while (expectedIterator.hasNext()) {
      TupleEntry tupleEntry = (TupleEntry) expectedIterator.next();
      expectedTuples.add(tupleEntry.getTupleCopy());
    }

    if (actualTuples.size() != expectedTuples.size()) {
      Assert.fail("Tail: " + tailName + ": Tuples differ in length: expected=" + expectedTuples.size()
        + " actual=" + actualTuples.size());
    }

    // Sort the data
    final Comparator<Tuple> sort = new Comparator<Tuple>() {
      @Override
      public int compare(Tuple o1, Tuple o2) {
        if (o1 == null) {
          return -1;
        }
        return o1.compareTo(getComparators(sf, o1), o2);
      }
    };
    Collections.sort(actualTuples, sort);
    Collections.sort(expectedTuples, sort);

    // Assert the tuples
    for (int i = 0; i < expectedTuples.size(); i++) {
      final Tuple expected = expectedTuples.get(i);
      final Tuple actual = actualTuples.get(i);

      if (expected == null) {
        assertTrue("Tail: " + tailName + ": Expected=null, but actual=" + actual, actual == null);
      } else {
        assertEquals("Tail: " + tailName + ": expected=" + expected + " actual=" + actual, 0,
          expected.compareTo(getComparators(sf, expected), actual));
      }
    }
  }

  /**
   * Returns the set of {@link Comparator}s for the given {@link Tuple}'s elements.
   * @param sf the {@link SerializationFactory} to search a {@link Comparison} for
   * @param tuple the expected {@link Tuple}
   * @return the set of {@link Comparator}s
   */
  private static Comparator[] getComparators(SerializationFactory sf, Tuple tuple) {
    final Comparator[] c = new Comparator[tuple.size()];

    for (int i = 0; i < c.length; i++) {
      final Object o = tuple.getObject(i);
      if (o == null) {
        c[i] = null;
      } else {
        final Serialization<?> ser = sf.getSerialization(o.getClass());
        if (ser instanceof Comparison) {
          c[i] = ((Comparison) ser).getComparator(o.getClass());
        } else {
          c[i] = null;
        }
      }
    }
    return c;
  }

  /**
   * Asserts that both {@link BufferedReader} are equal line by line.
   */
  public static void assertReadersEquals(final BufferedReader expected, final BufferedReader actual)
  throws Exception {
    String lineExpected = null;
    String lineActual = null;

    try {
      int lineNb = 0;
      while ((lineExpected = expected.readLine()) != null &&
             (lineActual = actual.readLine()) != null) {
        lineNb++;
        assertEquals("Line" + lineNb, lineExpected, lineActual);
      }
      lineActual = actual.readLine(); // check that the actual file ends also
      assertEquals("Line" + lineNb, lineExpected, lineActual);
    } finally {
      expected.close();
      actual.close();
    }
  }

  /**
   * Create a {@link HFile} into the output folder from the input data.
   * Returns an empty string if no {@link HFile} was created; the path to the {@link HFile} otherwise.
   * <p>
   * The input data must have two {@link Fields}, which {@link FieldType types} are described using the given
   * types argument.
   * @param properties the {@link Properties} to pass to the {@link HFileDictionary} creation {@link Flow}s
   * @param types the set of {@link FieldType}s
   * @param input the dictionary
   * @param output the directory to write the hfile into
   * @return the path to the generated {@link HFileDictionary}
   * @see HFileScheme
   * @see HFileDictionary
   * @see AnalyticsParameters#HFILE_PREFIX
   */
  public static String createHFile(final Properties properties,
                                   final Class[] types,
                                   final String input,
                                   final File output)
  throws IOException {
    if (types == null || types.length != 2) {
      throw new IllegalArgumentException("Invalid types specification for the HFile creation: " + types);
    }
    if (input == null) {
      return null;
    }

    final Fields fields = new Fields("key", "value");
    Pipe pipe = new Each("create-hfile", new ConvertFunction(fields, types));
    pipe = new Each(pipe, new Insert(new Fields("sort"), 1), Fields.ALL);
    pipe = new GroupBy(pipe, new Fields("sort"), new Fields("key"));
    pipe = new Each(pipe, fields, new Identity());

    final String out = new File(output, "hfile" + Math.random()).getAbsolutePath();
    final Tap source = getTap(input, fields);
    final Tap sink = new Hfs(new HFileScheme(fields), out);

    final Flow flow = new HadoopFlowConnector(properties).connect(source, sink, pipe);
    flow.complete();

    // get the filename
    final File[] files = new File(out).listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith(AnalyticsParameters.HFILE_PREFIX.get());
      }
    });
    if (files.length == 1) {
      return files[0].getAbsolutePath();
    } else if (files.length == 0) {
      return "";
    } else {
      throw new AnalyticsException("Found several matches of the prefix ["
      + AnalyticsParameters.HFILE_PREFIX.get() + "]: " + Arrays.toString(files));
    }
  }

}
