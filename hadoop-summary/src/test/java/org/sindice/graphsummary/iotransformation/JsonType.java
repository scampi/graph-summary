/**
 * Copyright (c) 2013 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.graphsummary.iotransformation;

import java.util.Map;
import java.util.TreeMap;

import org.codehaus.jackson.map.ObjectMapper;
import org.sindice.core.analytics.testHelper.iotransformation.AbstractFieldType;
import org.sindice.core.analytics.testHelper.iotransformation.FieldType;

import cascading.flow.FlowProcess;

/**
 * This {@link FieldType} formats the JSON string.
 */
public class JsonType
extends AbstractFieldType<String> {

  public JsonType(FlowProcess fp, String input) {
    super(fp, input);
  }

  @Override
  public String doConvert() {
    final ObjectMapper json = new ObjectMapper();
    try {
      final Map<String, Object> map = json.readValue(input, Map.class);
      final TreeMap<String, Object> sort = new TreeMap<String, Object>();
      sort.putAll(map);
      return json.writeValueAsString(sort);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected String getEmptyField() {
    return "{}";
  }

}
