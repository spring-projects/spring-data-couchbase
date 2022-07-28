/*
 * Copyright 2012-2020 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.repository.query;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.core.query.N1QLQuery;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.util.StreamUtils;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonValue;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * Abstract base for all Couchbase {@link RepositoryQuery}. It is in charge of inspecting the parameters and choosing
 * the correct {@link N1QLQuery} implementation to use.
 *
 * @author Simon Baslé
 * @author Subhashni Balakrishnan
 * @author Mark Paluch
 * @author Johannes Jasper
 */
public abstract class AbstractN1qlBasedQuery implements RepositoryQuery {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractN1qlBasedQuery.class);

	protected final CouchbaseQueryMethod queryMethod;
	private final CouchbaseOperations couchbaseOperations;

	protected AbstractN1qlBasedQuery(CouchbaseQueryMethod queryMethod, CouchbaseOperations couchbaseOperations) {
		this.queryMethod = queryMethod;
		this.couchbaseOperations = couchbaseOperations;
	}

	protected static N1QLQuery buildQuery(N1QLExpression expression, JsonValue queryPlaceholderValues,
			QueryScanConsistency scanConsistency) {
		QueryOptions opts = QueryOptions.queryOptions().scanConsistency(scanConsistency);

		if (queryPlaceholderValues instanceof JsonObject && !((JsonObject) queryPlaceholderValues).isEmpty()) {
			opts.parameters((JsonObject) queryPlaceholderValues);
		} else if (queryPlaceholderValues instanceof JsonArray && !((JsonArray) queryPlaceholderValues).isEmpty()) {
			opts.parameters((JsonArray) queryPlaceholderValues);
		}

		return new N1QLQuery(expression, opts);
	}

	/**
	 * The statement for a count() query. This must aggregate using count with the alias
	 * {@link CountFragment#COUNT_ALIAS}.
	 *
	 * @see CountFragment
	 */
	protected abstract N1QLExpression getCount(ParameterAccessor accessor, Object[] runtimeParameters);

	/**
	 * @return true if the {@link #getCount(ParameterAccessor, Object[]) count statement} should also be used when the
	 *         return type of the QueryMethod is a primitive type.
	 */
	protected abstract boolean useGeneratedCountQuery();

	protected abstract N1QLExpression getExpression(ParameterAccessor accessor, Object[] runtimeParameters,
			ReturnedType returnedType);

	protected abstract JsonValue getPlaceholderValues(ParameterAccessor accessor);

	protected QueryScanConsistency getScanConsistency() {
		return QueryScanConsistency.REQUEST_PLUS;
		/*
		if (queryMethod.hasConsistencyAnnotation()) {
		  return queryMethod.getConsistencyAnnotation().value();
		}
		
		return getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();*/
	}

	@Override
	public Object execute(Object[] parameters) {
		ParametersParameterAccessor accessor = new ParametersParameterAccessor(queryMethod.getParameters(), parameters);
		ResultProcessor processor = this.queryMethod.getResultProcessor().withDynamicProjection(accessor);
		ReturnedType returnedType = processor.getReturnedType();

		// TODO: review this - I just hacked it to work, basically...
		// This was what was here in sdk2, but seem to end up being always Object. Forcing
		// it to be the same as the object type for the repo.
		// Class<?> typeToRead = returnedType.getTypeToRead();
		// typeToRead = typeToRead == null ? returnedType.getDomainType() : typeToRead;

		Class<?> typeToRead = queryMethod.getEntityInformation().getJavaType();

		N1QLExpression statement = getExpression(accessor, parameters, returnedType);
		JsonValue queryPlaceholderValues = getPlaceholderValues(accessor);

		// prepare the final query
		N1QLQuery query = buildQuery(statement, queryPlaceholderValues, getScanConsistency());

		// prepare a count query
		N1QLExpression countStatement = getCount(accessor, parameters);
		// the place holder values are the same for the count query as well
		N1QLQuery countQuery = buildQuery(countStatement, queryPlaceholderValues, getScanConsistency());
		return processor
				.processResult(executeDependingOnType(query, countQuery, queryMethod, accessor.getPageable(), typeToRead));
	}

	protected Object executeDependingOnType(N1QLQuery query, N1QLQuery countQuery, QueryMethod queryMethod,
			Pageable pageable, Class<?> typeToRead) {

		if (queryMethod.isPageQuery()) {
			return executePaged(query, countQuery, pageable, typeToRead);
		} else if (queryMethod.isSliceQuery()) {
			return executeSliced(query, countQuery, pageable, typeToRead);
		} else if (queryMethod.isCollectionQuery()) {
			return executeCollection(query, typeToRead);
		} else if (queryMethod.isStreamQuery()) {
			return executeStream(query, typeToRead);
		} else if (queryMethod.isQueryForEntity()) {
			return executeEntity(query, typeToRead);
		} else if (queryMethod.getReturnedObjectType().isPrimitive() && useGeneratedCountQuery()) {
			// attempt to execute the created COUNT query
			return executeSingleProjection(countQuery);
		} else {
			// attempt a single projection on a simple type
			// (ie, a single row with a single k->v entry where v is the desired value)
			return executeSingleProjection(query);
		}
		// more complex projections could be added in the future, like DTO direct mapping with a SELECT a,b,c FROM something
	}

	private void logIfNecessary(N1QLQuery query) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Executing N1QL query: " + query.n1ql());
		}
	}

	protected List<?> executeCollection(N1QLQuery query, Class<?> typeToRead) {
		throw new UnsupportedOperationException("TODO");

		/*    logIfNecessary(query);
		List<?> result = couchbaseOperations.findByN1QL(query, typeToRead);
		return result;*/
	}

	protected Object executeEntity(N1QLQuery query, Class<?> typeToRead) {
		logIfNecessary(query);
		List<?> result = executeCollection(query, typeToRead);
		return result.isEmpty() ? null : result.get(0);
	}

	protected Object executeStream(N1QLQuery query, Class<?> typeToRead) {
		logIfNecessary(query);
		return StreamUtils.createStreamFromIterator(executeCollection(query, typeToRead).iterator());
	}

	protected Object executePaged(N1QLQuery query, N1QLQuery countQuery, Pageable pageable, Class<?> typeToRead) {
		throw new UnsupportedOperationException("TODO");
		/*
		Assert.notNull(pageable, "Pageable must not be null!");
		
		long total = 0L;
		logIfNecessary(countQuery);
		List<CountFragment> countResult = couchbaseOperations.findByN1QLProjection(countQuery, CountFragment.class);
		if (countResult != null && !countResult.isEmpty()) {
		  total = countResult.get(0).count;
		}
		
		logIfNecessary(query);
		List<?> result = couchbaseOperations.findByN1QL(query, typeToRead);
		return new PageImpl(result, pageable, total);*/
	}

	protected Object executeSliced(N1QLQuery query, N1QLQuery countQuery, Pageable pageable, Class<?> typeToRead) {
		throw new UnsupportedOperationException("TODO");

		/*    Assert.notNull(pageable, "Pageable must not be null!");
		logIfNecessary(query);
		List<?> result = couchbaseOperations.findByN1QL(query, typeToRead);
		int pageSize = pageable.getPageSize();
		boolean hasNext = result.size() > pageSize;
		
		return new SliceImpl(hasNext ? result.subList(0, pageSize) : result, pageable, hasNext);*/
	}

	protected Object executeSingleProjection(N1QLQuery query) {
		throw new UnsupportedOperationException("TODO");

		/*    logIfNecessary(query);
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
		
		return rowValues.iterator().next();*/
	}

	@Override
	public CouchbaseQueryMethod getQueryMethod() {
		return this.queryMethod;
	}

	protected CouchbaseOperations getCouchbaseOperations() {
		return this.couchbaseOperations;
	}
}
