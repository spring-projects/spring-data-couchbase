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
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.util.StreamUtils;
import org.springframework.util.Assert;

/**
 * Abstract base for all Couchbase {@link RepositoryQuery}. It is in charge of inspecting the parameters
 * and choosing the correct {@link N1qlQuery} implementation to use.
 */
public abstract class AbstractN1qlBasedQuery implements RepositoryQuery {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractN1qlBasedQuery.class);

  protected final CouchbaseQueryMethod queryMethod;
  private final CouchbaseOperations couchbaseOperations;

  protected AbstractN1qlBasedQuery(CouchbaseQueryMethod queryMethod, CouchbaseOperations couchbaseOperations) {
    this.queryMethod = queryMethod;
    this.couchbaseOperations = couchbaseOperations;
  }

  protected abstract Statement getCount(ParameterAccessor accessor);

  protected abstract Statement getStatement(ParameterAccessor accessor);

  protected abstract JsonArray getPlaceholderValues(ParameterAccessor accessor);

  @Override
  public Object execute(Object[] parameters) {
    ParameterAccessor accessor = new ParametersParameterAccessor(queryMethod.getParameters(), parameters);
    Statement statement = getStatement(accessor);
    JsonArray queryPlaceholderValues = getPlaceholderValues(accessor);

    //prepare the final query
    N1qlQuery query = buildQuery(statement, queryPlaceholderValues,
        getCouchbaseOperations().getDefaultConsistency().n1qlConsistency());

    //prepare a count query
    //TODO only do that when necessary (isPageQuery or isSliceQuery)
    Statement countStatement = getCount(accessor);
    N1qlQuery countQuery = buildQuery(countStatement, queryPlaceholderValues,
        getCouchbaseOperations().getDefaultConsistency().n1qlConsistency());

    return executeDependingOnType(query, countQuery, queryMethod, accessor.getPageable(),
        queryMethod.isPageQuery(), queryMethod.isSliceQuery(), queryMethod.isModifyingQuery());
  }

  protected static N1qlQuery buildQuery(Statement statement, JsonArray queryPlaceholderValues, ScanConsistency scanConsistency) {
    N1qlParams n1qlParams = N1qlParams.build().consistency(scanConsistency);
    N1qlQuery query;
    if (!queryPlaceholderValues.isEmpty()) {
      query = N1qlQuery.parameterized(statement, queryPlaceholderValues, n1qlParams);
    }
    else {
      query = N1qlQuery.simple(statement, n1qlParams);
    }
    return query;
  }

  protected Object executeDependingOnType(N1qlQuery query, N1qlQuery countQuery, QueryMethod queryMethod, Pageable pageable,
                                          boolean isPage, boolean isSlice, boolean isModifying) {
    if (isModifying) {
      throw new UnsupportedOperationException("Modifying queries not yet supported");
    }

    if (isPage) {
      return executePaged(query, countQuery, pageable);
    } else if (isSlice) {
      return executeSliced(query, countQuery, pageable);
    } else if (queryMethod.isCollectionQuery()) {
      return executeCollection(query);
    } else if (queryMethod.isQueryForEntity()) {
      return executeEntity(query);
    } else {
      return executeStream(query);
    }
  }

  private void logIfNecessary(N1qlQuery query) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Executing N1QL query: " + query.n1ql());
    }
  }

  protected List<?> executeCollection(N1qlQuery query) {
    logIfNecessary(query);
    List<?> result = couchbaseOperations.findByN1QL(query, queryMethod.getEntityInformation().getJavaType());
    return result;
  }

  protected Object executeEntity(N1qlQuery query) {
    logIfNecessary(query);
    List<?> result = executeCollection(query);
    return result.isEmpty() ? null : result.get(0);
  }

  protected Object executeStream(N1qlQuery query) {
    logIfNecessary(query);
    return StreamUtils.createStreamFromIterator(executeCollection(query).iterator());
  }

  protected Object executePaged(N1qlQuery query, N1qlQuery countQuery, Pageable pageable) {
    Assert.notNull(pageable);
    long total = 0L;
    logIfNecessary(countQuery);
    List<CountFragment> countResult = couchbaseOperations.findByN1QLProjection(countQuery, CountFragment.class);
    if (countResult != null && !countResult.isEmpty()) {
      total = countResult.get(0).count;
    }

    logIfNecessary(query);
    List<?> result = couchbaseOperations.findByN1QL(query, queryMethod.getEntityInformation().getJavaType());
    return new PageImpl(result, pageable, total);
  }

  protected Object executeSliced(N1qlQuery query, N1qlQuery countQuery, Pageable pageable) {
    Assert.notNull(pageable);
    logIfNecessary(query);
    List<?> result = couchbaseOperations.findByN1QL(query, queryMethod.getEntityInformation().getJavaType());
    int pageSize = pageable.getPageSize();
    boolean hasNext = result.size() > pageSize;

    return new SliceImpl(hasNext ? result.subList(0, pageSize) : result, pageable, hasNext);
  }

  @Override
  public CouchbaseQueryMethod getQueryMethod() {
    return this.queryMethod;
  }

  protected CouchbaseOperations getCouchbaseOperations() {
    return this.couchbaseOperations;
  }
}
