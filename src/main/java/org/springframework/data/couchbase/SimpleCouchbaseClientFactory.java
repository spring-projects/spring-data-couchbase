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

package org.springframework.data.couchbase;

import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.couchbase.core.CouchbaseExceptionTranslator;

import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;

public class SimpleCouchbaseClientFactory implements CouchbaseClientFactory {

	private final Cluster cluster;
	private final Bucket bucket;
	private final Scope scope;
	private final PersistenceExceptionTranslator exceptionTranslator;

	public SimpleCouchbaseClientFactory(final String connectionString, final Authenticator authenticator,
			final String bucketName) {
		this(Cluster.connect(connectionString, ClusterOptions.clusterOptions(authenticator)), bucketName, null);
	}

	public SimpleCouchbaseClientFactory(final String connectionString, final Authenticator authenticator,
			final String bucketName, final String scopeName) {
		this(Cluster.connect(connectionString, ClusterOptions.clusterOptions(authenticator)), bucketName, scopeName);
	}

	SimpleCouchbaseClientFactory(final Cluster cluster, final String bucketName, final String scopeName) {
		this.cluster = cluster;
		this.bucket = cluster.bucket(bucketName);
		this.scope = scopeName == null ? bucket.defaultScope() : bucket.scope(scopeName);
		this.exceptionTranslator = new CouchbaseExceptionTranslator();
	}

	public CouchbaseClientFactory withScope(final String scopeName) {
		return new SimpleCouchbaseClientFactory(cluster, bucket.name(), scopeName);
	}

	@Override
	public Cluster getCluster() {
		return cluster;
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
		if (collectionName == null) {
			if (!scope.name().equals(CollectionIdentifier.DEFAULT_SCOPE)) {
				throw new IllegalStateException("A collectionName must be provided if a non-default scope is used!");
			}
			return getBucket().defaultCollection();
		}
		return scope.collection(collectionName);
	}

	@Override
	public PersistenceExceptionTranslator getExceptionTranslator() {
		return exceptionTranslator;
	}

	@Override
	public void close() {
		cluster.disconnect();
	}

}
