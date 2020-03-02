/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.cascading;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.sindice.graphsummary.cascading.hash2value.rdf.DataGraphSummaryVocab.ANY23_PREFIX;
import static org.sindice.graphsummary.cascading.hash2value.rdf.DataGraphSummaryVocab.DGS_PREFIX;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.openrdf.model.Resource;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.ntriples.NTriplesUtil;
import org.openrdf.sail.memory.MemoryStore;
import org.openrdf.sail.memory.model.MemValueFactory;
import org.sindice.graphsummary.cascading.hash2value.rdf.DataGraphSummaryVocab;

/**
 * This class provides utility methods for running SPARQL queries over a
 * {@link Repository}.
 */
public class SparqlHelper {

  /**
   * {@link Pair} stores a solution to a binding name.
   */
  public static class Pair implements Comparable<Pair> {
    final String binding;
    final String value;
    public Pair(String binding, String value) {
      this.binding = binding;
      this.value = value;
    }
    @Override
    public String toString() {
      return "binding=" + binding + " value=" + value;
    }
    @Override
    public int compareTo(Pair o) {
      return this.binding.compareTo(o.binding);
    }
    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Pair) {
        final Pair o = (Pair) obj;
        return binding.equals(o.binding) && value.equals(o.value);
      }
      return false;
    }
    @Override
    public int hashCode() {
      int hash = 31 + (binding == null ? 0 : binding.hashCode());
      return hash * 31 + (value == null ? 0 : value.hashCode());
    }
  }

  /**
   * This method creates a {@link Repository} in memory and loads
   * the given N-Quads inside.
   * @param quads the {@link List} of quads to add to the repository
   * @return a {@link SailRepository}, backed with a {@link MemoryStore}
   * @throws Exception if something wrong happens while ingesting the quads
   */
  public static Repository createRepositoryFromQuads(final List<String> quads)
  throws Exception {
    final MemValueFactory memFactory = new MemValueFactory();
    final SailRepository repo = new SailRepository(new MemoryStore());
    repo.initialize();

    final SailRepositoryConnection con = repo.getConnection();
    con.setAutoCommit(true);
    try {
      for (String quad: quads) {
        final String q = quad.replaceFirst(" \\.$", "");
        final int endNT = q.lastIndexOf(' ');
        final String triple = q.substring(0, endNT) + " .\n";
        final String c = q.substring(endNT + 1, q.length());
        final Resource context = NTriplesUtil.parseResource(c, memFactory);
        con.add(new StringReader(triple), ANY23_PREFIX, RDFFormat.NTRIPLES, context);
      }
    } finally {
      con.close();
    }
    return repo;
  }

  /**
   * This method creates a {@link Repository} in memory and loads
   * the given N-Triples inside.
   * @param triples the {@link List} of triples to add to the repository
   * @return a {@link SailRepository}, backed with a {@link MemoryStore}
   * @throws Exception if something wrong happens while ingesting the triples
   */
  public static Repository createRepositoryFromTriples(final List<String> triples)
  throws Exception {
    final SailRepository repo = new SailRepository(new MemoryStore());
    repo.initialize();

    final SailRepositoryConnection con = repo.getConnection();
    con.setAutoCommit(true);
    try {
      for (final String triple: triples) {
        con.add(new StringReader(triple), ANY23_PREFIX, RDFFormat.NTRIPLES);
      }
    } finally {
      con.close();
    }
    return repo;
  }

  /**
   * Execute the a SPARQL query against the given {@link Repository}, and
   * asserts if the returned bindings are the ones expected.
   * <p>
   * This method wraps the given <b>where</b> clause into a <code>SELECT</code>
   * query, with the following <code>PREFIX</code>es:
   * <ul>
   * <li><b>an</b>: {@value DataGraphSummaryVocab#DGS_PREFIX}; and</li>
   * <li><b>any23</b>: {@value DataGraphSummaryVocab#ANY23_PREFIX}.</li>
   * </ul>
   * <p>
   * The <code>expectedBindings</code> is a {@link List} of tuples.
   * A <code>tuple</code> is a {@link List} of {@link Pair}, which
   * represents a row of the query's solutions.
   * 
   * @param whereClause the <code>WHERE</code> clause of the SPARQL query
   * @param repo the {@link Repository} to evaluate the query on
   * @param expectedBindings the expected {@link List} of tuples
   * @param distinct if <code>true</code>, the DISTINCT keyword is used on the SELECT query
   *                 ({@link #getSelectQuery(HashSet, String, boolean)})
   * @throws Exception if something wrong happens while evaluating the query.
   */
  public static void _assertQuery(final String whereClause,
                                  final Repository repo,
                                  final List<List<Pair>> expectedBindings,
                                  final boolean distinct)
  throws Exception {
    // Get the list of bindings
    final HashSet<String> bindingsName = new HashSet<String>();
    for (List<Pair> pairs: expectedBindings) {
      for (Pair pair: pairs) {
        bindingsName.add(pair.binding);
      }
    }

    // Execute the query
    final String query = getSelectQuery(bindingsName, whereClause, distinct);
    final List<List<Pair>> actualBindings = evaluateSparqlQuery(repo, query, bindingsName);

    // Assert that all bindings are correct
    _assertQueryResults(expectedBindings, actualBindings);
  }

  /**
   * Creates a SELECT query with the given WHERE clause, and
   * uses bindingsName as the variables to project.
   * @param distinct if <code>true</code>, the DISTINCT keyword is used on the SELECT query
   */
  private static String getSelectQuery(final HashSet<String> bindingsName,
                                       final String whereClause,
                                       final boolean distinct) {
    // Create the SELECT query
    String query = "PREFIX an: <" + DGS_PREFIX + ">\n" +
                   "PREFIX any23: <" + ANY23_PREFIX + ">\n" +
                   "SELECT " + (distinct ? "DISTINCT " : "");
    if (bindingsName.isEmpty()) {
      query += "* ";
    } else {
      for (String b: bindingsName) {
        query += "?" + b + " ";
      }
    }
    return query + "{\n" + whereClause + "}";
  }

  /**
   * Evaluate the SPARQL query over the repository.
   * Returns the list of tuples that have the solutions of the variables
   * in bindingsName.
   */
  private static List<List<Pair>> evaluateSparqlQuery(final Repository repo,
                                                      final String query,
                                                      final HashSet<String> bindingsName)
  throws Exception {
    final List<List<Pair>> actualBindings = new ArrayList<List<Pair>>();
    final RepositoryConnection con = repo.getConnection();

    try {
      final TupleQuery tq = con.prepareTupleQuery(QueryLanguage.SPARQL, query);
      final TupleQueryResult res = tq.evaluate();

      // Get the actual bindings
      while (res.hasNext()) {
        final BindingSet bindingSet = res.next();
        final Iterator<Binding> it = bindingSet.iterator();
        final List<Pair> tuple = new ArrayList<Pair>();
        while (it.hasNext()) {
          final Binding b = it.next();
          if (bindingsName.contains(b.getName())) {
            tuple.add(new Pair(b.getName(), b.getValue().stringValue()));
          }
        }
        if (!tuple.isEmpty()) {
          actualBindings.add(tuple);
        }
      }
    } finally {
      con.close();
    }
    return actualBindings;
  }

  /**
   * Assert that the actual results of the query are the ones expected.
   * @param expectedBindings the expected list of tuples
   * @param actualBindings the actual list of tuples
   */
  private static void _assertQueryResults(final List<List<Pair>> expectedBindings,
                                          final List<List<Pair>> actualBindings) {
    assertEquals(actualBindings.toString(), expectedBindings.size(), actualBindings.size());
    // Sort each tuple by order of the bindings name
    for (List<Pair> pair : actualBindings) {
      Collections.sort(pair);
    }
    for (List<Pair> pair : expectedBindings) {
      Collections.sort(pair);
    }
    // Sort the list of tuples
    final Comparator<List<Pair>> cmp = new Comparator<List<Pair>>() {
      @Override
      public int compare(List<Pair> o1, List<Pair> o2) {
        if (o1.size() != o2.size()) {
          throw new IllegalArgumentException("Comparing two tuples of different size:\n"
              + o1 + "\n" + o2);
        }
        if (o1.isEmpty()) {
          throw new IllegalArgumentException("Comparing empty tuples");
        }
        return o1.toString().compareTo(o2.toString());
      }
    };
    Collections.sort(actualBindings, cmp);
    Collections.sort(expectedBindings, cmp);
    assertArrayEquals(actualBindings.toString(), expectedBindings.toArray(), actualBindings.toArray());
  }

}
