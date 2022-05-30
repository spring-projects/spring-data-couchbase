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

import java.io.IOException;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.couchbase.core.CouchbaseExceptionTranslator;
import org.springframework.data.couchbase.transaction.SessionAwareMethodInterceptor;
import org.springframework.util.ObjectUtils;

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
	public ReactiveCouchbaseClientFactory withCore(ReactiveCouchbaseResourceHolder holder) {
		return new CoreTransactionAttemptContextBoundCouchbaseClientFactory(holder, this, transactions);
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

	/**
	 * {@link CoreTransactionAttemptContext} bound TODO decorating the database with a
	 * {@link SessionAwareMethodInterceptor}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	static final class CoreTransactionAttemptContextBoundCouchbaseClientFactory
			implements ReactiveCouchbaseClientFactory {

		private final ReactiveCouchbaseResourceHolder transactionResources;
		private final ReactiveCouchbaseClientFactory delegate;

		CoreTransactionAttemptContextBoundCouchbaseClientFactory(ReactiveCouchbaseResourceHolder transactionResources,
				ReactiveCouchbaseClientFactory delegate, Transactions transactions) {
			this.transactionResources = transactionResources;
			this.delegate = delegate;
		}


		@Override
		public ClusterInterface getCluster() throws DataAccessException {
			return decorateDatabase(delegate.getCluster());
		}

		@Override
		public Mono<Collection> getCollectionMono(String name) {
			return Mono.just(delegate.getCollection(name));
		}

		@Override
		public Collection getCollection(String collectionName) {
			return delegate.getCollection(collectionName);
		}

		@Override
		public Scope getScope(String scopeName) {
			return delegate.getScope(scopeName);
		}

		@Override
		public Scope getScope() {
			return delegate.getScope();
		}

		@Override
		public ReactiveCouchbaseClientFactory withScope(String scopeName) {
			return delegate.withScope(scopeName);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.ReactiveMongoDatabaseFactory#getExceptionTranslator()
		 */
		@Override
		public PersistenceExceptionTranslator getExceptionTranslator() {
			return delegate.getExceptionTranslator();
		}

		@Override
		public String getBucketName() {
			return delegate.getBucketName();
		}

		@Override
		public String getScopeName() {
			return delegate.getScopeName();
		}

		@Override
		public void close() throws IOException {
			delegate.close();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.ReactiveMongoDatabaseFactory#getSession(com.mongodb.CoreTransactionAttemptContextOptions)
		 */

		@Override
		public Mono<ReactiveCouchbaseResourceHolder> getResourceHolderMono() {
			return Mono.just(transactionResources);
		}

		@Override
		public ReactiveCouchbaseResourceHolder getResourceHolder(TransactionOptions options,
																														 CoreTransactionAttemptContext atr) {
			ReactiveCouchbaseResourceHolder holder = delegate.getResourceHolder(options, atr);
			return holder;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.ReactiveMongoDatabaseFactory#withSession(com.mongodb.session.CoreTransactionAttemptContext)
		 */
		@Override
		public ReactiveCouchbaseClientFactory withCore(ReactiveCouchbaseResourceHolder core) {
			return delegate.withCore(core);
		}

		@Override
		public CouchbaseTransactionalOperator getTransactionalOperator() {
			return delegate.getTransactionalOperator();
		}

		@Override
		public ReactiveCouchbaseClientFactory with(CouchbaseTransactionalOperator txOp) {
			return delegate.with(txOp);
		}

		private ClusterInterface decorateDatabase(ClusterInterface database) {
			return createProxyInstance(transactionResources, database, ClusterInterface.class);
		}

		private ClusterInterface proxyDatabase(ReactiveCouchbaseResourceHolder session, ClusterInterface database) {
			return createProxyInstance(session, database, ClusterInterface.class);
		}

		private Collection proxyCollection(ReactiveCouchbaseResourceHolder session, Collection collection) {
			return createProxyInstance(session, collection, Collection.class);
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

		public ReactiveCouchbaseClientFactory getDelegate() {
			return this.delegate;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;

			CoreTransactionAttemptContextBoundCouchbaseClientFactory that = (CoreTransactionAttemptContextBoundCouchbaseClientFactory) o;

			if (!ObjectUtils.nullSafeEquals(this.transactionResources, that.transactionResources)) {
				return false;
			}
			return ObjectUtils.nullSafeEquals(this.delegate, that.delegate);
		}

		@Override
		public int hashCode() {
			int result = ObjectUtils.nullSafeHashCode(this.transactionResources);
			result = 31 * result + ObjectUtils.nullSafeHashCode(this.delegate);
			return result;
		}

		public String toString() {
			return "SimpleReactiveCouchbaseDatabaseFactory.CoreTransactionAttemptContextBoundCouchDbFactory(session="
					+ this.getResourceHolderMono() + ", delegate=" + this.getDelegate() + ")";
		}
	}
}
