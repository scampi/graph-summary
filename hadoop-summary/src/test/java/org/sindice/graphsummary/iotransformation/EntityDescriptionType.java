/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */

package org.sindice.graphsummary.iotransformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.openrdf.model.BNode;
import org.openrdf.model.Statement;
import org.sindice.analytics.entity.AnalyticsValue;
import org.sindice.analytics.entity.BlankNode;
import org.sindice.analytics.entity.EntityDescription;
import org.sindice.analytics.entity.EntityDescriptionFactory;
import org.sindice.analytics.entity.EntityDescription.Statements;
import org.sindice.core.analytics.rdf.RDFParser;
import org.sindice.core.analytics.testHelper.iotransformation.AbstractFieldType;

import cascading.flow.FlowProcess;

/**
 * Create an {@link EntityDescription} using the implementation set in the {@link FlowProcess}.
 * <p>
 * The {@link BNode}s' label is encoded using {@link BlankNode}. In order to mimic this, the blank node
 * label in the test input is equal to the concatenation of the document URL with the original blank node
 * label.
 */
public class EntityDescriptionType
extends AbstractFieldType<EntityDescription> {

  public EntityDescriptionType(FlowProcess fp, String s) {
    super(fp, s);
  }

  @Override
  public EntityDescription doConvert() {
    final EntityDescription ed = EntityDescriptionFactory.getEntityDescription(fp);
    final RDFParser parser = new RDFParser(fp);

    final String[] inputSplitted = StringUtils.split(this.input, "\n");
    Arrays.sort(inputSplitted);
    for (int i = 0; i < inputSplitted.length; i++) {
      final Statement st = parser.parseStatement(inputSplitted[i]);
      if (st == null) {
        continue;
      }

      final AnalyticsValue s = AnalyticsValue.fromSesame(st.getSubject());
      final AnalyticsValue p = AnalyticsValue.fromSesame(st.getPredicate());
      final AnalyticsValue o = AnalyticsValue.fromSesame(st.getObject());

      if (s instanceof BNode) {
        s.setValue(BlankNode.encode("", s.stringValue()).getBytes());
      }
      if (o instanceof BNode) {
        o.setValue(BlankNode.encode("", o.stringValue()).getBytes());
      }

      ed.add(s, p, o);
    }
    return ed;
  }

  @Override
  protected EntityDescription doSort(EntityDescription data) {
    final EntityDescription ed = EntityDescriptionFactory.getEntityDescription(fp);
    final Statements it = data.iterateStatements();
    final Map<AnalyticsValue, List<AnalyticsValue>> triples = new TreeMap<AnalyticsValue, List<AnalyticsValue>>();

    // collect the entity
    final AnalyticsValue subject = data.getEntity();
    while (it.getNext()) {
      final AnalyticsValue predicate = it.getPredicateCopy();
      final AnalyticsValue object = it.getObjectCopy();

      if (!triples.containsKey(predicate)) {
        triples.put(predicate, new ArrayList<AnalyticsValue>());
      }
      triples.get(predicate).add(object);
    }
    // sort the statements and recreate the entity
    for (Entry<AnalyticsValue, List<AnalyticsValue>> entry : triples.entrySet()) {
      final AnalyticsValue p = entry.getKey();
      Collections.sort(entry.getValue());
      for (AnalyticsValue o : entry.getValue()) {
        ed.add(subject, p, o);
      }
    }
    return ed;
  }

  @Override
  protected EntityDescription getEmptyField() {
    return EntityDescriptionFactory.getEntityDescription(fp);
  }

}
