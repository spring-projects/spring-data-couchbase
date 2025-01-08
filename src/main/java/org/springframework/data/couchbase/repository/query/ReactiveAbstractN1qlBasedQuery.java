/*
 * Copyright 2017-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.repository.query;

import reactor.core.publisher.Flux;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.core.query.N1QLQuery;
import org.springframework.data.couchbase.repository.query.support.N1qlUtils;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.repository.util.ReactiveWrapperConverters;

import com.couchbase.client.java.json.JsonValue;
import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * @author Subhashni Balakrishnan
 * @author Mark Paluch
 * @author Johannes Jasper
 * @since 3.0
 */
public abstract class ReactiveAbstractN1qlBasedQuery implements RepositoryQuery {
	private static final Logger LOG = LoggerFactory.getLogger(ReactiveAbstractN1qlBasedQuery.class);

	protected final CouchbaseQueryMethod queryMethod;
	private final ReactiveCouchbaseOperations couchbaseOperations;

	protected ReactiveAbstractN1qlBasedQuery(CouchbaseQueryMethod method, ReactiveCouchbaseOperations operations) {
		this.queryMethod = method;
		this.couchbaseOperations = operations;
	}

	protected abstract N1QLExpression getExpression(ParameterAccessor accessor, Object[] runtimeParameters,
			ReturnedType returnedType);

	protected abstract JsonValue getPlaceholderValues(ParameterAccessor accessor);

	@Override
	public Object execute(Object[] parameters) {
		ReactiveCouchbaseParameterAccessor accessor = new ReactiveCouchbaseParameterAccessor(queryMethod, parameters);

		return accessor.resolveParameters().flatMapMany(it -> {
		ResultProcessor processor = this.queryMethod.getResultProcessor().withDynamicProjection(accessor);
		ReturnedType returnedType = processor.getReturnedType();

		Class<?> typeToRead = returnedType.getTypeToRead();
		typeToRead = typeToRead == null ? returnedType.getDomainType() : typeToRead;

		N1QLExpression expression = getExpression(accessor, parameters, returnedType);
		JsonValue queryPlaceholderValues = getPlaceholderValues(accessor);

		// prepare the final query
		N1QLQuery query = N1qlUtils.buildQuery(expression, queryPlaceholderValues, getScanConsistency());
		return ReactiveWrapperConverters
				.toWrapper(processor.processResult(executeDependingOnType(query, queryMethod, typeToRead)), Flux.class);
		});
	}

	protected Object executeDependingOnType(N1QLQuery query, QueryMethod queryMethod, Class<?> typeToRead) {

		if (queryMethod.isModifyingQuery()) {
			throw new UnsupportedOperationException("Modifying queries not yet supported");
		}

		if (queryMethod.isQueryForEntity()) {
			return execute(query, typeToRead);
		} else {
			return executeSingleProjection(query, typeToRead);
		}
	}

	private void logIfNecessary(N1QLQuery query) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Executing N1QL query: " + query.n1ql());
		}
	}

	protected Object execute(N1QLQuery query, Class<?> typeToRead) {
		throw new UnsupportedOperationException();
		/*
		    logIfNecessary(query);
		    return couchbaseOperations.findByN1QL(query, typeToRead);*/
	}

	protected Object executeSingleProjection(N1QLQuery query, final Class<?> typeToRead) {
		throw new UnsupportedOperationException();

		/*        logIfNecessary(query);
		    return couchbaseOperations.findByN1QLProjection(query, Map.class)
		            .map(m -> {
		                    if (m.size() > 1) {
		                        throw new CouchbaseQueryExecutionException("Query returning primitive got more values than expected: "
		                                + m.size());
		                    }
		                    Object v = m.values().iterator().next();
		                    return this.couchbaseOperations.getConverter().getConversionService().convert(v, typeToRead);
		                });*/
	}

	@Override
	public CouchbaseQueryMethod getQueryMethod() {
		return this.queryMethod;
	}

	protected ReactiveCouchbaseOperations getCouchbaseOperations() {
		return this.couchbaseOperations;
	}

	protected QueryScanConsistency getScanConsistency() {
		throw new UnsupportedOperationException();

		/*
		    if (queryMethod.hasConsistencyAnnotation()) {
		    return queryMethod.getConsistencyAnnotation().value();
		  }

		  return getCouchbaseOperations().getDefaultConsistency().n1qlConsistency();*/
	}
}
