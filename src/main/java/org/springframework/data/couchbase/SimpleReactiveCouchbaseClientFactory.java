package org.springframework.data.couchbase;

import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_COLLECTION;
import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_SCOPE;

import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.java.ClusterInterface;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.transactions.AttemptContextReactiveAccessor;
import com.couchbase.client.java.transactions.Transactions;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import org.springframework.data.couchbase.transaction.CouchbaseTransactionalOperator;
import org.springframework.data.couchbase.transaction.ReactiveCouchbaseResourceHolder;
import reactor.core.publisher.Mono;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.couchbase.core.CouchbaseExceptionTranslator;
import org.springframework.data.couchbase.transaction.SessionAwareMethodInterceptor;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;

public class SimpleReactiveCouchbaseClientFactory implements ReactiveCouchbaseClientFactory {
	final ClusterInterface cluster;
	final String bucketName;
	final String scopeName;
	final PersistenceExceptionTranslator exceptionTranslator;
	JsonSerializer serializer;
	Transactions transactions;
	CouchbaseTransactionalOperator transactionalOperator;

	public SimpleReactiveCouchbaseClientFactory(Cluster cluster, String bucketName, String scopeName,
			CouchbaseTransactionalOperator transactionalOperator) {
		this.cluster = cluster;
		this.bucketName = bucketName;
		this.scopeName = scopeName;
		this.exceptionTranslator = new CouchbaseExceptionTranslator();
		this.serializer = cluster.environment().jsonSerializer();
		this.transactions = cluster.transactions();
		this.transactionalOperator = transactionalOperator;
	}

	public SimpleReactiveCouchbaseClientFactory(Cluster cluster, String bucketName, String scopeName) {
		this(cluster, bucketName, scopeName, null);
	}


	@Override
	public ClusterInterface getCluster() {
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

	@Override public Scope getScope(){
		return getScope(null);
	}

	@Override
	public String getScopeName() {
		return scopeName;
	}

	@Override
	public Mono<Collection> getCollectionMono(String collectionName) {
		if (getScopeName() != null && !DEFAULT_SCOPE.equals(getScopeName())) {
			if (collectionName == null || DEFAULT_COLLECTION.equals(collectionName)) {
				throw new IllegalStateException("A collectionName must be provided if a non-default scope is used.");
			}
		}
		if (getScopeName() == null || DEFAULT_SCOPE.equals(getScopeName())) {
			if (collectionName != null && !DEFAULT_COLLECTION.equals(collectionName)) {
				throw new IllegalStateException(
						"A collectionName must be null or " + DEFAULT_COLLECTION + " if scope is null or " + DEFAULT_SCOPE);
			}
		}
		return Mono.just(getScope()).map((s) -> s.collection(collectionName != null ? collectionName : DEFAULT_COLLECTION));
	}

	@Override
	public Collection getCollection(String collectionName) {
		if (getScopeName() != null && !DEFAULT_SCOPE.equals(getScopeName())) {
			if (collectionName == null || DEFAULT_COLLECTION.equals(collectionName)) {
				throw new IllegalStateException("A collectionName must be provided if a non-default scope is used.");
			}
		}
		if (getScopeName() == null || DEFAULT_SCOPE.equals(getScopeName())) {
			if (collectionName != null && !DEFAULT_COLLECTION.equals(collectionName)) {
				throw new IllegalStateException(
						"A collectionName must be null or " + DEFAULT_COLLECTION + " if scope is null or " + DEFAULT_SCOPE);
			}
		}
		return cluster.bucket(bucketName).scope(scopeName != null ? scopeName : DEFAULT_SCOPE)
				.collection(collectionName != null ? collectionName : DEFAULT_COLLECTION);
	}

	@Override
	public ReactiveCouchbaseClientFactory withScope(String scopeName) {
		return new SimpleReactiveCouchbaseClientFactory((Cluster) cluster, bucketName,
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
	public Mono<ReactiveCouchbaseResourceHolder> getResourceHolderMono() {
		return Mono.just(new ReactiveCouchbaseResourceHolder(null));
	}

	@Override
	public ReactiveCouchbaseResourceHolder getResourceHolder(TransactionOptions options,
																													 CoreTransactionAttemptContext atr) {
		if (atr == null) {
			atr = AttemptContextReactiveAccessor
					.newCoreTranactionAttemptContext(AttemptContextReactiveAccessor.reactive(transactions));
		}
		return new ReactiveCouchbaseResourceHolder(atr);
	}

	@Override
	public CouchbaseTransactionalOperator getTransactionalOperator() {
		return transactionalOperator;
	}

	@Override
	public ReactiveCouchbaseClientFactory with(CouchbaseTransactionalOperator txOp) {
		return new SimpleReactiveCouchbaseClientFactory((Cluster) getCluster(), bucketName, scopeName, txOp);
	}

	private <T> T createProxyInstance(ReactiveCouchbaseResourceHolder session, T target, Class<T> targetType) {

		ProxyFactory factory = new ProxyFactory();
		factory.setTarget(target);
		factory.setInterfaces(targetType);
		factory.setOpaque(true);

		factory.addAdvice(new SessionAwareMethodInterceptor<>(session, target, ReactiveCouchbaseResourceHolder.class,
				ClusterInterface.class, this::proxyDatabase, Collection.class, this::proxyCollection));

		return targetType.cast(factory.getProxy(target.getClass().getClassLoader()));
	}

	private Collection proxyCollection(ReactiveCouchbaseResourceHolder session, Collection c) {
		return createProxyInstance(session, c, Collection.class);
	}

	private ClusterInterface proxyDatabase(ReactiveCouchbaseResourceHolder session, ClusterInterface cluster) {
		return createProxyInstance(session, cluster, ClusterInterface.class);
	}
}
