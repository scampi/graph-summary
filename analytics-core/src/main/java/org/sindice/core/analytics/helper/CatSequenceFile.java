/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.helper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.sindice.core.analytics.cascading.AbstractAnalyticsCLI.InputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.TextLine;
import cascading.scheme.hadoop.WritableSequenceFile;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

/**
 * The {@link CatSequenceFile} prints the content of a {@link SequenceFile}.
 */
public class CatSequenceFile {

  private final Logger        logger  = LoggerFactory.getLogger(CatSequenceFile.class);

  private final FileStatus[]  sfDir;
  public static Display       DISPLAY = Display.PRETTY_PRINT;
  public static String        SEP     = "";

  private final Configuration conf;

  /**
   * The different kinds of output format.
   */
  public enum Display {
    /** Pretty output */
    PRETTY_PRINT,
    /** As it is output */
    PLAIN,
    /** {@link #PLAIN} with each values separated by the {@link CatSequenceFile#SEP} character */
    SEP
  }

  /**
   * This enum defines if a {@link Fields} is to be printed or not.
   */
  public enum FilterEnum {
    MUST, MUST_NOT
  }

  /**
   * This class defines a user-defined value for a {@link Fields}.
   */
  public class Filter {
    FilterEnum occur;
    String     value;
  }

  /**
   * Create a {@link CatSequenceFile} instance.
   * @param conf the {@link Configuration} to read the {@link SequenceFile} with
   * @param sfDir the path to the {@link SequenceFile}s
   * @param regex the {@link SequenceFile}s filename regular expression.
   */
  public CatSequenceFile(Configuration conf, Path sfDir, final String regex) throws IOException {
    this.conf = conf;
    this.sfDir = sfDir.getFileSystem(conf).listStatus(sfDir, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().matches(regex);
      }
    });
  }

  /**
   * Prints the content of the given {@link SequenceFile}.
   * @param format the {@link InputFormat}
   * @param filters the set of {@link Fields} on which to apply the {@link Filter}s
   * @param fieldsOp the set of {@link Fields} on which to apply boolean operations
   * @param merges the list of {@link Fields} to merge on a single line
   */
  public void cat(final InputFormat format,
                  final Map<String, Filter> filters,
                  final Map<String, Filter> fieldsOp,
                  final List<String[]> merges)
  throws IOException {
    HadoopFlowProcess flowProcess = new HadoopFlowProcess((JobConf) conf);
    logger.info("{} input files", sfDir.length);
    for (FileStatus src : sfDir) {
      final Scheme scheme;
      switch (format) {
        case CASCADING_SEQUENCE_FILE:
          scheme = new SequenceFile(Fields.ALL);
          break;
        case HADOOP_SEQUENCE_FILE:
          scheme = new WritableSequenceFile(new Fields("fields0", "fields1"), Writable.class, Writable.class);
          break;
        case TEXTLINE:
          scheme = new TextLine(new Fields("fields0"));
          break;
        default:
          throw new EnumConstantNotPresentException(InputFormat.class, format.toString());
      }
      final TupleEntryIterator tuple = new Hfs(scheme, src.getPath().toUri().toString()).openForRead(flowProcess);
      try {
        while (tuple.hasNext()) {
          final TupleEntry entry = tuple.next();
          boolean print = true;

          final TreeMap<String, String> fields = new TreeMap<String, String>();
          for (int pos = 0; pos < entry.size(); pos++) { // for each
            // field
            // value
            final String fname = "fields" + pos;
            final String value = String.valueOf(entry.getObject(pos));
            // field value filters
            if (filters.containsKey(fname)) {
              if (filters.get(fname).occur.equals(FilterEnum.MUST) && !filters.get(fname).value.equals(value)) {
                print = false;
                break;
              } else if (filters.get(fname).occur.equals(FilterEnum.MUST_NOT) && filters.get(fname).value.equals(value)) {
                print = false;
                break;
              }
            }
            // fields operators
            if (fieldsOp.containsKey(fname)) {
              if (fieldsOp.get(fname).occur.equals(FilterEnum.MUST) &&
                  !entry.getString(fieldsOp.get(fname).value).equals(value)) {
                print = false;
                break;
              } else if (fieldsOp.get(fname).occur.equals(FilterEnum.MUST_NOT) &&
                         entry.getString(fieldsOp.get(fname).value).equals(value)) {
                print = false;
                break;
              }
            }
            switch (DISPLAY) {
              case PRETTY_PRINT:
                fields.put(fname, fname + ": " + value + "$");
                break;
              case PLAIN:
              case SEP:
                fields.put(fname, value + SEP);
                break;
              default:
                throw new EnumConstantNotPresentException(Display.class, DISPLAY.toString());
            }
          }
          if (print) {
            if (merges == null) {
              for (String field : fields.values()) {
                System.out.println(field);
              }
            } else {
              StringBuilder sb = new StringBuilder();
              for (String[] field : merges) {
                sb.setLength(0);
                for (String f : field) {
                  sb.append(fields.get(f));
                }
                System.out.println(sb.toString());
              }
            }
            System.out.println();
          }
        }
      }
      finally {
        tuple.close();
      }
    }
  }

}
