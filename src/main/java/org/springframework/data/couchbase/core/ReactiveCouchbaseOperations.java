/*
 * Copyright 2012-2025 the original author or authors
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
package org.springframework.data.couchbase.core;

import reactor.core.publisher.Mono;

import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.query.Query;

import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * Defines common operations on the Couchbase data source, most commonly implemented by
 * {@link ReactiveCouchbaseTemplate}.
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
public interface ReactiveCouchbaseOperations extends ReactiveFluentCouchbaseOperations {

	/**
	 * Returns the converter used for this template/operations.
	 */
	CouchbaseConverter getConverter();

	/**
	 * The name of the bucket used.
	 */
	String getBucketName();

	/**
	 * The name of the scope used, null if the default scope is used.
	 */
	String getScopeName();

	/**
	 * Returns the underlying client factory.
	 */
	CouchbaseClientFactory getCouchbaseClientFactory();

	/**
	 * Save the entity to couchbase.<br>
	 * If there is no version property on the entity class, and this is in a transaction, use insert. <br>
	 * If there is no version property on the entity class, and this is not in a transaction, use upsert. <br>
	 * If there is a version property on the entity class, and it is non-zero, then this is an existing document, use
	 * replace.<br>
	 * Otherwise, there is a version property for the entity, but it is zero or null, use insert. <br>
	 *
	 * @param entity the entity to save in couchbase
	 * @param scopeAndCollection for use by repositories only. these are varargs for the scope and collection.
	 * @param <T> the entity class
	 * @return
	 */
	<T> Mono<T> save(T entity, String... scopeAndCollection);

	/**
	 * Returns the count of documents found by the query.
	 * @param query
	 * @param domainType
	 * @param <T>
	 * @return
	 */
	<T> Mono<Long> count(Query query, Class<T> domainType);

	/**
	 * @return the default consistency to use for queries
	 */
	QueryScanConsistency getConsistency();
}
