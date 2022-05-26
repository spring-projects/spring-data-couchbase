/*
 * Copyright 2016-2021 the original author or authors.
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
package org.springframework.data.couchbase;

import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterInterface;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.transactions.ReactiveTransactionAttemptContext;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import org.springframework.data.couchbase.transaction.ClientSessionOptions;
import org.springframework.data.couchbase.transaction.CouchbaseTransactionalOperator;
import org.springframework.data.couchbase.transaction.ReactiveCouchbaseResourceHolder;
import reactor.core.publisher.Mono;

import org.springframework.dao.support.PersistenceExceptionTranslator;

import java.io.IOException;

/**
 * Interface for factories creating reactive {@link Cluster} instances.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Mathieu Ouellet
 * @since 2.0
 */
public interface ReactiveCouchbaseClientFactory /*extends CodecRegistryProvider*/ {

	/**
	 * Provides access to the managed SDK {@link Cluster} reference.
	 */
	Mono<ClusterInterface> getCluster();

	/**
	 * Provides access to the managed SDK {@link Cluster} reference.
	 */
	ClusterInterface getBlockingCluster();

	/**
	 * Provides access to the managed SDK {@link Bucket} reference.
	 */
	Mono<Bucket> getBucket();

	/**
	 * Provides access to the managed SDK {@link Scope} reference.
	 */
	Mono<Scope> getScope();

	/**
	 * Provides access to the managed SDK {@link Scope} reference without block()
	 */
	 Scope getBlockingScope(String scopeName);

	/**
	 * Provides access to a collection (identified by its name) in managed SDK {@link Scope} reference.
	 *
	 * @param name the name of the collection. If null is passed in, the default collection is assumed.
	 */
	Mono<Collection> getCollection(String name);

	/**
	 * Provides access to a collection (identified by its name) without block()
	 *
	 * @param name the name of the collection. If null is passed in, the default collection is assumed.
	 */
	 Collection getBlockingCollection(String collectionName);

	/**
	 * Provides access to the default collection.
	 */
	Mono<Collection> getDefaultCollection();

	/**
	 * Returns a new {@link CouchbaseClientFactory} set to the scope given as an argument.
	 *
	 * @param scopeName the name of the scope to use for all collection access.
	 * @return a new client factory, bound to the other scope.
	 */
	ReactiveCouchbaseClientFactory withScope(String scopeName);

	/**
	 * The exception translator used on the factory.
	 */
	PersistenceExceptionTranslator getExceptionTranslator();

	Mono<ReactiveCouchbaseResourceHolder> getTransactionResources(TransactionOptions options);

	String getBucketName();

	String getScopeName();

	void close() throws IOException;

	ReactiveCouchbaseResourceHolder getTransactionResources(TransactionOptions options,
			CoreTransactionAttemptContext ctx);

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.ReactiveMongoDatabaseFactory#withSession(com.mongodb.session.ClientSession)
	 */
	ReactiveCouchbaseClientFactory withCore(ReactiveCouchbaseResourceHolder core);

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.ReactiveMongoDatabaseFactory#with(com.mongodb.session.ClientSession)
	 */
	ReactiveCouchbaseClientFactory with(CouchbaseTransactionalOperator txOp);

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.ReactiveMongoDatabaseFactory#isTransactionActive()
	 */
	boolean isTransactionActive();

	CouchbaseTransactionalOperator getTransactionalOperator();
}
