/*
 * Copyright 2012-2021 the original author or authors
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
package org.springframework.data.couchbase;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.function.Supplier;

import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.couchbase.core.CouchbaseExceptionTranslator;

import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.OwnedSupplier;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.transactions.AttemptContextReactiveAccessor;
import com.couchbase.client.java.transactions.config.TransactionsCleanupConfig;
import com.couchbase.client.java.transactions.config.TransactionsConfig;

/**
 * The default implementation of a {@link CouchbaseClientFactory}.
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
public class SimpleCouchbaseClientFactory implements CouchbaseClientFactory {

	private final Supplier<Cluster> cluster;
	private final Bucket bucket;
	private final Scope scope;
	private final PersistenceExceptionTranslator exceptionTranslator;
	//private JsonSerializer serializer = null;

	public SimpleCouchbaseClientFactory(final String connectionString, final Authenticator authenticator,
										final String bucketName) {
		this(connectionString, authenticator, bucketName, null);
	}

	public SimpleCouchbaseClientFactory(final String connectionString, final Authenticator authenticator,
										final String bucketName, final String scopeName) {
		this(new OwnedSupplier<>(Cluster.connect(connectionString, ClusterOptions.clusterOptions(authenticator)
						// todo gp disabling cleanupLostAttempts to simplify output during development
						.environment(env -> env.transactionsConfig(
								TransactionsConfig.cleanupConfig(TransactionsCleanupConfig.cleanupLostAttempts(false)))))),
				bucketName, scopeName);
	}

	public SimpleCouchbaseClientFactory(final String connectionString, final Authenticator authenticator,
										final String bucketName, final String scopeName, final ClusterEnvironment environment) {
		this(
				new OwnedSupplier<>(
						Cluster.connect(connectionString, ClusterOptions.clusterOptions(authenticator).environment(environment))),
				bucketName, scopeName);
		//this.serializer = environment.jsonSerializer();
	}

	public SimpleCouchbaseClientFactory(final Cluster cluster, final String bucketName, final String scopeName) {
		this(() -> cluster, bucketName, scopeName);
		//this.serializer = cluster.environment().jsonSerializer();
	}

	private SimpleCouchbaseClientFactory(final Supplier<Cluster> cluster, final String bucketName,
										 final String scopeName) {
		this.cluster = cluster;
		this.bucket = cluster.get().bucket(bucketName);
		this.scope = scopeName == null ? bucket.defaultScope() : bucket.scope(scopeName);
		this.exceptionTranslator = new CouchbaseExceptionTranslator();
	}

	@Override
	public CouchbaseClientFactory withScope(final String scopeName) {
		return new SimpleCouchbaseClientFactory(cluster, bucket.name(), scopeName != null ? scopeName : getScope().name());
	}

	@Override
	public Cluster getCluster() {
		return cluster.get();
	}

	@Override
	public Bucket getBucket() {
		return bucket;
	}

	@Override
	public Scope getScope() {
		return scope;
	}

	@Override
	public Collection getCollection(final String collectionName) {
		final Scope scope = getScope();
		if (collectionName == null || CollectionIdentifier.DEFAULT_COLLECTION.equals(collectionName)) {
			if (!scope.name().equals(CollectionIdentifier.DEFAULT_SCOPE)) {
				throw new IllegalStateException("A collectionName must be provided if a non-default scope is used");
			}
			return getBucket().defaultCollection();
		}
		return scope.collection(collectionName);
	}

	@Override
	public Collection getDefaultCollection() {
		return getCollection(null);
	}

	@Override
	public PersistenceExceptionTranslator getExceptionTranslator() {
		return exceptionTranslator;
	}

	@Override
	public CoreTransactionAttemptContext getCore(TransactionOptions options, CoreTransactionAttemptContext atr) {
		// can't we just use AttemptContextReactive everywhere? Instead of creating AttemptContext(atr), then
		// accessing at.getACR() ?
		if (atr == null) {
			atr = AttemptContextReactiveAccessor
					.newCoreTranactionAttemptContext(AttemptContextReactiveAccessor.reactive(getCluster().transactions()));
		}

		return atr;
	}

	// @Override
	// public CouchbaseClientFactory with(CouchbaseStuffHandle txOp) {
	// return new SimpleCouchbaseClientFactory(cluster, bucket.name(), scope.name(), txOp);
	// }

	// @Override
	// public CouchbaseStuffHandle getTransactionalOperator() {
	// return (CouchbaseStuffHandle) transactionalOperator;
	// }

	@Override
	public void close() {
		// todo gp
		// if (cluster instanceof OwnedSupplier) {
		// cluster.get().disconnect();
		// }
	}

	private static Duration now() {
		return Duration.of(System.nanoTime(), ChronoUnit.NANOS);
	}

}
