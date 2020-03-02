/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.riffle;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

import riffle.process.Process;
import riffle.process.scheduler.ProcessException;
import cascading.flow.hadoop.ProcessFlow;
import cascading.management.state.ClientState;
import cascading.stats.FlowStats;

/**
 * {@link RiffleProcessFlow} allows to pass statistics within a {@link Process}
 * back to the wrapping {@link ProcessFlow}.
 */
public class RiffleProcessFlow<P>
extends ProcessFlow<P> {

  public RiffleProcessFlow(String name, P process) {
    super(name, process);
  }

  @Override
  public FlowStats getFlowStats() {
    final ClientState clientState = getFlowSession().getCascadingServices().createClientState(getID());
    final ProcessFlowStats flowStats = new ProcessFlowStats(this, clientState);

    try {
      final List<FlowStats> stats = (List<FlowStats>) findInvoke(ProcessStats.class, true);
      if (stats == null) {
        return this.flowStats;
      }
      flowStats.setFlowStatsList(stats);
    } catch (ProcessException e) {
      throw new IllegalArgumentException("Unable to get the stats of the Riffle process", e);
    }
    return flowStats;
  }

  private Object findInvoke(Class<? extends Annotation> type, boolean isOptional)
  throws ProcessException {
    Method method = null;

    try {
      method = findMethodWith(type, isOptional);

      if (method == null)
        return null;

      return invokeMethod(method);
    } catch (InvocationTargetException exception) {
      throw new ProcessException(type, method, exception);
    }
  }

  private Method findMethodWith(Class<? extends Annotation> type, boolean isOptional) {
    Method[] methods = getProcess().getClass().getMethods();

    for (Method method : methods) {
      if (method.getAnnotation(type) == null)
        continue;

      int modifiers = method.getModifiers();

      if (Modifier.isAbstract(modifiers))
        throw new IllegalStateException("Given process method: " + method.getName() + " must not be abstract");

      if (Modifier.isInterface(modifiers))
        throw new IllegalStateException("Given process method: " + method.getName() + " must be implemented");

      if (!Modifier.isPublic(modifiers))
        throw new IllegalStateException("Given process method: " + method.getName() + " must be public");

      return method;
    }

    if (!isOptional)
      throw new IllegalStateException("No method found declaring annotation: " + type.getName());

    return null;
  }

  private Object invokeMethod(Method method)
  throws InvocationTargetException {
    try {
      return method.invoke(getProcess());
    } catch (IllegalAccessException exception) {
      throw new IllegalStateException("Unable to invoke method: " + method.getName(), exception);
    } catch (InvocationTargetException exception) {
      if (exception.getCause() instanceof Error)
        throw (Error) exception.getCause();

      if (exception.getCause() instanceof RuntimeException)
        throw (RuntimeException) exception.getCause();

      throw exception;
    }
  }

}
