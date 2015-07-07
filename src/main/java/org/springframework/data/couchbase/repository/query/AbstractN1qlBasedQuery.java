/*
 * Copyright 2012-2015 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.repository.query;

import java.util.List;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.query.Statement;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.util.StreamUtils;

/**
 * Abstract base for all Couchbase {@link RepositoryQuery}. It is in charge of inspecting the parameters
 * and choosing the correct {@link Query} implementation to use.
 */
public abstract class AbstractN1qlBasedQuery implements RepositoryQuery {

  protected final CouchbaseQueryMethod queryMethod;
  private final CouchbaseOperations couchbaseOperations;

  protected AbstractN1qlBasedQuery(CouchbaseQueryMethod queryMethod, CouchbaseOperations couchbaseOperations) {
    this.queryMethod = queryMethod;
    this.couchbaseOperations = couchbaseOperations;
  }

  protected abstract Statement getStatement(ParameterAccessor accessor);

  protected abstract JsonArray getPlaceholderValues(ParameterAccessor accessor);

  @Override
  public Object execute(Object[] parameters) {
    ParameterAccessor accessor = new ParametersParameterAccessor(queryMethod.getParameters(), parameters);
    Statement statement = getStatement(accessor);
    JsonArray queryPlaceholderValues = getPlaceholderValues(accessor);

    Query query = buildQuery(statement, queryPlaceholderValues);
    return executeDependingOnType(query, queryMethod, queryMethod.isPageQuery(), queryMethod.isModifyingQuery(),
        queryMethod.isSliceQuery());
  }

  protected static Query buildQuery(Statement statement, JsonArray queryPlaceholderValues) {
    Query query;
    if (!queryPlaceholderValues.isEmpty()) {
      query = Query.parametrized(statement, queryPlaceholderValues);
    }
    else {
      query = Query.simple(statement);
    }

    return query;
  }

  protected Object executeDependingOnType(Query query, QueryMethod queryMethod,
                                          boolean isPage, boolean isSlice, boolean isModifying) {
    if (isPage || isSlice || isModifying) {
      throw new UnsupportedOperationException("Slice, page and modifying queries not yet supported");
    }

    if (queryMethod.isCollectionQuery()) {
      return executeCollection(query);
    } else if (queryMethod.isQueryForEntity()) {
      return executeEntity(query);
    } else {
      return executeStream(query);
    }
  }

  protected List<?> executeCollection(Query query) {
    List<?> result = couchbaseOperations.findByN1QL(query, queryMethod.getEntityInformation().getJavaType());
    return result;
  }

  protected Object executeEntity(Query query) {
    List<?> result = executeCollection(query);
    return result.isEmpty() ? null : result.get(0);
  }

  protected Object executeStream(Query query) {
    return StreamUtils.createStreamFromIterator(executeCollection(query).iterator());
  }

  @Override
  public CouchbaseQueryMethod getQueryMethod() {
    return this.queryMethod;
  }

  protected CouchbaseOperations getCouchbaseOperations() {
    return this.couchbaseOperations;
  }
}
