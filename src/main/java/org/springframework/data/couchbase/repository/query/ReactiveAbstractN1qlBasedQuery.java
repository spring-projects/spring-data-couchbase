/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.repository.query;

import java.util.Map;
import com.couchbase.client.java.document.json.JsonValue;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.RxJavaCouchbaseOperations;
import org.springframework.data.couchbase.repository.query.support.N1qlUtils;
import org.springframework.data.repository.query.*;
import org.springframework.data.repository.util.ReactiveWrapperConverters;
import reactor.core.publisher.Flux;

/**
 * @author Subhashni Balakrishnan
 * @since 3.0
 */
public abstract class ReactiveAbstractN1qlBasedQuery implements RepositoryQuery {
    private static final Logger LOG = LoggerFactory.getLogger(ReactiveAbstractN1qlBasedQuery.class);

    protected final CouchbaseQueryMethod queryMethod;
    private final RxJavaCouchbaseOperations couchbaseOperations;

    protected ReactiveAbstractN1qlBasedQuery(CouchbaseQueryMethod method, RxJavaCouchbaseOperations operations) {
        this.queryMethod = method;
        this.couchbaseOperations = operations;
    }

    protected abstract Statement getStatement(ParameterAccessor accessor, Object[] runtimeParameters, ReturnedType returnedType);

    protected abstract JsonValue getPlaceholderValues(ParameterAccessor accessor);

    @Override
    public Object execute(Object[] parameters) {
        ReactiveCouchbaseParameterAccessor accessor = new ReactiveCouchbaseParameterAccessor(queryMethod, parameters);
        ResultProcessor processor = this.queryMethod.getResultProcessor().withDynamicProjection(accessor);
        ReturnedType returnedType = processor.getReturnedType();

        Class<?> typeToRead = returnedType.getTypeToRead();
        typeToRead = typeToRead == null ? returnedType.getDomainType() : typeToRead;

        Statement statement = getStatement(accessor, parameters, returnedType);
        JsonValue queryPlaceholderValues = getPlaceholderValues(accessor);

        //prepare the final query
        N1qlQuery query = N1qlUtils.buildQuery(statement, queryPlaceholderValues,
                getCouchbaseOperations().getDefaultConsistency().n1qlConsistency());
        return ReactiveWrapperConverters.toWrapper(
                processor.processResult(executeDependingOnType(query, queryMethod, typeToRead)), Flux.class);
    }


    protected Object executeDependingOnType(N1qlQuery query,
                                            QueryMethod queryMethod,
                                            Class<?> typeToRead) {

        if (queryMethod.isModifyingQuery()) {
            throw new UnsupportedOperationException("Modifying queries not yet supported");
        }

        if (queryMethod.isQueryForEntity()) {
            return execute(query, typeToRead);
        } else {
            return executeSingleProjection(query);
        }
    }

    private void logIfNecessary(N1qlQuery query) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Executing N1QL query: " + query.n1ql());
        }
    }

    protected Object execute(N1qlQuery query, Class<?> typeToRead) {
        logIfNecessary(query);
        return couchbaseOperations.findByN1QL(query, typeToRead);
    }

    protected Object executeSingleProjection(N1qlQuery query) {
        logIfNecessary(query);
        return couchbaseOperations.findByN1QLProjection(query, Map.class);
    }

    @Override
    public CouchbaseQueryMethod getQueryMethod() {
        return this.queryMethod;
    }

    protected RxJavaCouchbaseOperations getCouchbaseOperations() {
        return this.couchbaseOperations;
    }
}
