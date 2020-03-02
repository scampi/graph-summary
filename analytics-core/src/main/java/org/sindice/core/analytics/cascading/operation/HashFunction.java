/**
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 */
package org.sindice.core.analytics.cascading.operation;

import java.util.ArrayList;

import org.sindice.core.analytics.util.Hash;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * This {@link Function} {@link Hash hashes} the input {@link Fields} value.
 */
public class HashFunction
extends BaseOperation<HashFunction.Context>
implements Function<HashFunction.Context> {

  private static final long serialVersionUID = -9081363615925234455L;

  class Context {
    final Tuple             result   = new Tuple();
    final ArrayList<Object> elements = new ArrayList<Object>();
  }

  /** The number of bits to hash the value with */
  private final int nHashBits;

  public HashFunction() {
    this(new Fields("key"), 64);
  }

  public HashFunction(int nHashBytes) {
    this(new Fields("key"), nHashBytes);
  }

  public HashFunction(final Fields fieldDeclaration) {
    // expects 1 arguments, fail otherwise
    this(fieldDeclaration, 64);
  }

  public HashFunction(final Fields fieldDeclaration, int nHashBytes) {
    // expects 1 arguments, fail otherwise
    super(1, fieldDeclaration);
    this.nHashBits = nHashBytes;
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Context> functionCall) {
    final TupleEntry arguments = functionCall.getArguments();
    final Context c = functionCall.getContext();

    c.elements.clear();
    for (int i = 0; i < arguments.size(); i++) {
      c.elements.add(arguments.getObject(i));
    }

    c.result.clear();
    if (nHashBits == 32) {
      c.result.addAll(Hash.getHash32(c.elements));
    } else if (nHashBits == 64) {
      if (c.elements.size() == 1) {
        c.result.addAll(Hash.getHash64(c.elements.get(0)));
      } else if (c.elements.size() == 2) {
        c.result.addAll(Hash.getHash64(c.elements.get(0), c.elements.get(1)));
      } else {
        c.result.addAll(Hash.getHash64(c.elements));
      }
    } else if (nHashBits == 128) {
      c.result.addAll(Hash.getHash128(c.elements));
    } else {
      return;
    }
    functionCall.getOutputCollector().add(c.result);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Context> operationCall) {
    operationCall.setContext(new Context());
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof HashFunction)) {
      return false;
    }
    if (!super.equals(object)) {
      return false;
    }

    final HashFunction hash = (HashFunction) object;
    return hash.nHashBits == nHashBits;
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = hash * 31 + nHashBits;
    return hash;
  }

}
