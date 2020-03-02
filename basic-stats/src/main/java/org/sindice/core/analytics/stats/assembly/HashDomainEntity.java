/*******************************************************************************
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 *
 *
 * This project is a free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This project is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this project. If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.sindice.core.analytics.stats.assembly;

import org.sindice.core.analytics.util.Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Adds a field with the domain of the uri in object
 */
public class HashDomainEntity
extends BaseOperation<HashDomainEntity.Context>
implements Function<HashDomainEntity.Context> {

  private static final long   serialVersionUID = 1L;

  private static final Logger logger           = LoggerFactory
                                               .getLogger(HashDomainEntity.class);

  private final int           nHashBytes;

  public static class Context {
    Tuple result = new Tuple();
  }

  public HashDomainEntity(final Fields fieldDeclaration) {
    this(fieldDeclaration, 64);
  }

  public HashDomainEntity(final Fields fieldDeclaration, int nhashBytes) {
    super(2, fieldDeclaration);
    this.nHashBytes = nhashBytes;
  }

  private boolean isBlankNode(String uri) {
    return (uri.length() < 2 ? false : uri.charAt(0) == '_'
        && uri.charAt(1) == ':');
  }

  @Override
  public void operate(FlowProcess flowProcess,
                      FunctionCall<Context> functionCall) {
    final TupleEntry arguments = functionCall.getArguments();
    final Context c = functionCall.getContext();
    final String entity = arguments.getString(0);
    final String url = arguments.getString(1);

    c.result.clear();
    if (nHashBytes == 64) {
      if (isBlankNode(entity.toString())) {
        c.result.addAll(Hash.getHash64(url, entity));
      } else {
        c.result.addAll(Hash.getHash64(entity));
      }
    } else if (nHashBytes == 128) {
      if (isBlankNode(entity.toString())) {
        c.result.addAll(Hash.getHash128(url, entity));
      } else {
        c.result.addAll(Hash.getHash128(entity));
      }
    } else {
      logger.error("HashFunction produces only long or int hashes");
      return;
    }
    functionCall.getOutputCollector().add(c.result);
  }

  @Override
  public void prepare(FlowProcess flowProcess,
                      OperationCall<Context> operationCall) {
    operationCall.setContext(new Context());
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof HashDomainEntity)) {
      return false;
    }
    if (!super.equals(object)) {
      return false;
    }

    final HashDomainEntity hash = (HashDomainEntity) object;
    return hash.nHashBytes == nHashBytes;
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = hash * 31 + nHashBytes;
    return hash;
  }

}
