package org.springframework.data.couchbase;

import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_COLLECTION;
import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_SCOPE;

import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.transactions.AttemptContextReactiveAccessor;
import com.couchbase.client.java.transactions.Transactions;
import org.springframework.data.couchbase.transaction.CouchbaseResourceHolder;
import reactor.core.publisher.Mono;

import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.couchbase.core.CouchbaseExceptionTranslator;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;

public class SimpleReactiveCouchbaseClientFactory implements ReactiveCouchbaseClientFactory {
	final Cluster cluster;
	final String bucketName;
	final String scopeName;
	final PersistenceExceptionTranslator exceptionTranslator;
	JsonSerializer serializer;
	Transactions transactions;

	public SimpleReactiveCouchbaseClientFactory(Cluster cluster, String bucketName, String scopeName) {
		this.cluster = cluster;
		this.bucketName = bucketName;
		this.scopeName = scopeName;
		this.exceptionTranslator = new CouchbaseExceptionTranslator();
		this.serializer = cluster.environment().jsonSerializer();
		this.transactions = cluster.transactions();
	}

	@Override
	public Cluster getCluster() {
		return cluster;
	}

	@Override
	public String getBucketName() {
		return bucketName;
	}

	@Override
	public Scope getScope(String scopeName) {
		return cluster.bucket(bucketName)
				.scope(scopeName != null ? scopeName : (this.scopeName != null ? this.scopeName : DEFAULT_SCOPE));
	}

	@Override
	public Scope getScope() {
		return getScope(null);
	}

	@Override
	public String getScopeName() {
		return scopeName;
	}

	@Override
	public Mono<Collection> getCollectionMono(String collectionName) {
		return Mono.just(getScope()).map((s) -> s.collection(getCollectionName(collectionName)));
	}

	@Override
	public Collection getCollection(String collectionName) {
		return cluster.bucket(bucketName).scope(getScopeNameForCollection(collectionName))
				.collection(getCollectionName(collectionName));
	}

	private String getScopeNameForCollection(String collectionName){
		if (getScopeName() != null && !DEFAULT_SCOPE.equals(getScopeName())) {
			if (collectionName == null || DEFAULT_COLLECTION.equals(collectionName)) {
				throw new IllegalStateException("A collectionName must be provided if a non-default scope is used.");
			}
		}
		return scopeName != null ? scopeName : DEFAULT_SCOPE;
	}

	private String getCollectionName(String collectionName) {
		if (getScopeName() == null || DEFAULT_SCOPE.equals(getScopeName())) {
			if (collectionName != null && !DEFAULT_COLLECTION.equals(collectionName)) {
				throw new IllegalStateException(
						"A collectionName must be null or " + DEFAULT_COLLECTION + " if scope is null or " + DEFAULT_SCOPE);
			}
		}
		return collectionName != null ? collectionName : DEFAULT_COLLECTION;
	}

	@Override
	public ReactiveCouchbaseClientFactory withScope(String scopeName) {
		return new SimpleReactiveCouchbaseClientFactory(cluster, bucketName,
				scopeName != null ? scopeName : this.scopeName);
	}

	@Override
	public PersistenceExceptionTranslator getExceptionTranslator() {
		return exceptionTranslator;
	}

	@Override
	public void close() {
		cluster.disconnect();
	}

	@Override
	public CouchbaseResourceHolder getResources() {
		return new CouchbaseResourceHolder(null);
	}

	@Override
	public CouchbaseResourceHolder getResources(CoreTransactionAttemptContext atr) {
		if (atr == null) {
			atr = AttemptContextReactiveAccessor
					.newCoreTranactionAttemptContext(AttemptContextReactiveAccessor.reactive(transactions));
		}
		return new CouchbaseResourceHolder(atr);
	}

}
