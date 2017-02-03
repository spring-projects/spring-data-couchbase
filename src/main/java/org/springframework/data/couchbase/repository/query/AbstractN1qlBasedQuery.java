/*
 * Copyright 2012-2017 the original author or authors
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.document.json.JsonValue;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.CouchbaseQueryExecutionException;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.util.StreamUtils;
import org.springframework.util.Assert;

/**
 * Abstract base for all Couchbase {@link RepositoryQuery}. It is in charge of inspecting the parameters
 * and choosing the correct {@link N1qlQuery} implementation to use.
 *
 * @author Simon Baslé
 * @author Subhashni Balakrishnan
 * @author Mark Paluch
 */
public abstract class AbstractN1qlBasedQuery implements RepositoryQuery {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractN1qlBasedQuery.class);

  protected final CouchbaseQueryMethod queryMethod;
  private final CouchbaseOperations couchbaseOperations;

  protected AbstractN1qlBasedQuery(CouchbaseQueryMethod queryMethod, CouchbaseOperations couchbaseOperations) {
    this.queryMethod = queryMethod;
    this.couchbaseOperations = couchbaseOperations;
  }

  /**
   * The statement for a count() query. This must aggregate using count with the alias {@link CountFragment#COUNT_ALIAS}.
   *
   * @see CountFragment
   */
  protected abstract Statement getCount(ParameterAccessor accessor, Object[] runtimeParameters);

  /**
   * @return true if the {@link #getCount(ParameterAccessor, Object[]) count statement} should also be used when
   * the return type of the QueryMethod is a primitive type.
   */
  protected abstract boolean useGeneratedCountQuery();

  protected abstract Statement getStatement(ParameterAccessor accessor, Object[] runtimeParameters, ReturnedType returnedType);

  protected abstract JsonValue getPlaceholderValues(ParameterAccessor accessor);

  @Override
  public Object execute(Object[] parameters) {
    ParametersParameterAccessor accessor = new ParametersParameterAccessor(queryMethod.getParameters(), parameters);

    ResultProcessor processor = this.queryMethod.getResultProcessor().withDynamicProjection(Optional.of(accessor));
    ReturnedType returnedType = processor.getReturnedType();
    
    Class<?> typeToRead = returnedType.getTypeToRead();
    typeToRead = typeToRead == null ? returnedType.getDomainType() : typeToRead;

    Statement statement = getStatement(accessor, parameters, returnedType);
    JsonValue queryPlaceholderValues = getPlaceholderValues(accessor);

    //prepare the final query
    N1qlQuery query = buildQuery(statement, queryPlaceholderValues,
        getCouchbaseOperations().getDefaultConsistency().n1qlConsistency());

    //prepare a count query
    Statement countStatement = getCount(accessor, parameters);
    N1qlQuery countQuery = buildQuery(countStatement, queryPlaceholderValues,
        getCouchbaseOperations().getDefaultConsistency().n1qlConsistency());
    return processor.processResult(executeDependingOnType(query, countQuery, queryMethod, accessor.getPageable(), typeToRead));
  }

  protected static N1qlQuery buildQuery(Statement statement, JsonValue queryPlaceholderValues, ScanConsistency scanConsistency) {
    N1qlParams n1qlParams = N1qlParams.build().consistency(scanConsistency);
    N1qlQuery query;

    if (queryPlaceholderValues instanceof JsonObject && !((JsonObject) queryPlaceholderValues).isEmpty()) {
      query = N1qlQuery.parameterized(statement, (JsonObject) queryPlaceholderValues, n1qlParams);
    } else if (queryPlaceholderValues instanceof JsonArray && !((JsonArray) queryPlaceholderValues).isEmpty()) {
      query = N1qlQuery.parameterized(statement, (JsonArray) queryPlaceholderValues, n1qlParams);
    } else {
      query = N1qlQuery.simple(statement, n1qlParams);
    }
    return query;
  }

  protected Object executeDependingOnType(N1qlQuery query, N1qlQuery countQuery, QueryMethod queryMethod,
      Pageable pageable, Class<?> typeToRead) {

    if (queryMethod.isModifyingQuery()) {
      throw new UnsupportedOperationException("Modifying queries not yet supported");
    }

    if (queryMethod.isPageQuery()) {
      return executePaged(query, countQuery, pageable, typeToRead);
    } else if (queryMethod.isSliceQuery()) {
      return executeSliced(query, countQuery, pageable, typeToRead);
    } else if (queryMethod.isCollectionQuery()) {
      return executeCollection(query, typeToRead);
    } else if (queryMethod.isStreamQuery()){
      return executeStream(query, typeToRead);
    } else if (queryMethod.isQueryForEntity()) {
      return executeEntity(query, typeToRead);
    } else if (queryMethod.getReturnedObjectType().isPrimitive()
        && useGeneratedCountQuery()) {
      //attempt to execute the created COUNT query
      return executeSingleProjection(countQuery);
    } else {
      //attempt a single projection on a simple type
      // (ie, a single row with a single k->v entry where v is the desired value)
      return executeSingleProjection(query);
    }
    //more complex projections could be added in the future, like DTO direct mapping with a SELECT a,b,c FROM something
  }

  private void logIfNecessary(N1qlQuery query) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Executing N1QL query: " + query.n1ql());
    }
  }

  protected List<?> executeCollection(N1qlQuery query, Class<?> typeToRead) {
    logIfNecessary(query);
    List<?> result = couchbaseOperations.findByN1QL(query, typeToRead);
    return result;
  }

  protected Object executeEntity(N1qlQuery query, Class<?> typeToRead) {
    logIfNecessary(query);
    List<?> result = executeCollection(query, typeToRead);
    return result.isEmpty() ? null : result.get(0);
  }

  protected Object executeStream(N1qlQuery query, Class<?> typeToRead) {
    logIfNecessary(query);
    return StreamUtils.createStreamFromIterator(executeCollection(query, typeToRead).iterator());
  }

  protected Object executePaged(N1qlQuery query, N1qlQuery countQuery, Pageable pageable, Class<?> typeToRead) {
    Assert.notNull(pageable, "Pageable must not be null!");

    long total = 0L;
    logIfNecessary(countQuery);
    List<CountFragment> countResult = couchbaseOperations.findByN1QLProjection(countQuery, CountFragment.class);
    if (countResult != null && !countResult.isEmpty()) {
      total = countResult.get(0).count;
    }

    logIfNecessary(query);
    List<?> result = couchbaseOperations.findByN1QL(query, typeToRead);
    return new PageImpl(result, pageable, total);
  }

  protected Object executeSliced(N1qlQuery query, N1qlQuery countQuery, Pageable pageable, Class<?> typeToRead) {
    Assert.notNull(pageable, "Pageable must not be null!");
    logIfNecessary(query);
    List<?> result = couchbaseOperations.findByN1QL(query, typeToRead);
    int pageSize = pageable.getPageSize();
    boolean hasNext = result.size() > pageSize;

    return new SliceImpl(hasNext ? result.subList(0, pageSize) : result, pageable, hasNext);
  }

  protected Object executeSingleProjection(N1qlQuery query) {
    logIfNecessary(query);
    //the structure of the response from N1QL gives us a JSON object even when selecting a single aggregation
    List<Map> resultAsMap = couchbaseOperations.findByN1QLProjection(query, Map.class);

    if (resultAsMap.size() != 1) {
      throw new CouchbaseQueryExecutionException("Query returning a primitive type are expected to return " +
          "exactly 1 result, got " + resultAsMap.size());
    }

    Map<String, Object> singleRow = (Map<String, Object>) resultAsMap.get(0);
    if (singleRow.size() != 1) {
      throw new CouchbaseQueryExecutionException("Query returning a simple type are expected to return " +
          "a unique value, got " + singleRow.size());
    }
    Collection<Object> rowValues = singleRow.values();
    if (rowValues.size() != 1) {
      throw new CouchbaseQueryExecutionException("Query returning a simple type are expected to use a " +
          "single aggregation/projection, got " + rowValues.size());
    }

    return rowValues.iterator().next();
  }

  @Override
  public CouchbaseQueryMethod getQueryMethod() {
    return this.queryMethod;
  }

  protected CouchbaseOperations getCouchbaseOperations() {
    return this.couchbaseOperations;
  }
}
