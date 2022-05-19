package org.springframework.data.couchbase;

import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_COLLECTION;
import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_SCOPE;

import com.couchbase.client.java.ClusterInterface;
import com.couchbase.client.java.transactions.ReactiveTransactionAttemptContext;
import org.springframework.data.couchbase.transaction.CouchbaseStuffHandle;
import reactor.core.publisher.Mono;

import java.io.IOException;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.couchbase.core.CouchbaseExceptionTranslator;
import org.springframework.data.couchbase.transaction.ClientSession;
import org.springframework.data.couchbase.transaction.ClientSessionImpl;
import org.springframework.data.couchbase.transaction.ClientSessionOptions;
import org.springframework.data.couchbase.transaction.SessionAwareMethodInterceptor;
import org.springframework.util.ObjectUtils;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;

public class SimpleReactiveCouchbaseClientFactory implements ReactiveCouchbaseClientFactory {
	final Mono<ClusterInterface> cluster;
	final String bucketName;
	final String scopeName;
	final PersistenceExceptionTranslator exceptionTranslator;
	//CouchbaseStuffHandle transactionalOperator;

	public SimpleReactiveCouchbaseClientFactory(Cluster cluster, String bucketName, String scopeName) {
		this(cluster, bucketName, scopeName, null);
	}

	public SimpleReactiveCouchbaseClientFactory(Cluster cluster, String bucketName, String scopeName,
			CouchbaseStuffHandle stuff) {
		this.cluster = Mono.just(cluster);
		this.bucketName = bucketName;
		this.scopeName = scopeName;
		this.exceptionTranslator = new CouchbaseExceptionTranslator();
		//this.transactionalOperator = stuff;
	}

	@Override
	public Mono<ClusterInterface> getCluster() {
		return cluster;
	}

	@Override
	public Mono<Bucket> getBucket() {
		return cluster.map((c) -> c.bucket(bucketName));
	}

	@Override
	public String getBucketName() {
		return bucketName;
	}

	@Override
	public Mono<Scope> getScope() {
		return cluster.map((c) -> c.bucket(bucketName).scope(scopeName != null ? scopeName : DEFAULT_SCOPE));
	}

	@Override
	public String getScopeName() {
		return scopeName;
	}

	@Override
	public Mono<Collection> getCollection(String collectionName) {
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
		return getScope().map((s) -> s.collection(collectionName != null ? collectionName : DEFAULT_COLLECTION));
	}

	@Override
	public Mono<Collection> getDefaultCollection() {
		if (getScopeName() != null && DEFAULT_SCOPE.equals(getScopeName())) {
			throw new IllegalStateException("A collectionName must be provided if a non-default scope is used.");
		}
		return cluster.map((c) -> c.bucket(bucketName).defaultCollection());
	}

	@Override
	public ReactiveCouchbaseClientFactory withScope(String scopeName) {
		return new SimpleReactiveCouchbaseClientFactory((Cluster) cluster.block(), bucketName,
				scopeName != null ? scopeName : this.scopeName);
	}

	@Override
	public PersistenceExceptionTranslator getExceptionTranslator() {
		return exceptionTranslator;
	}

	@Override
	public void close() {
		cluster.block().disconnect();
	}

	@Override
	public Mono<ClientSession> getSession(ClientSessionOptions options) { // hopefully this gets filled in later
		return Mono.from(Mono.just(new ClientSessionImpl(this, null))); // .startSession(options));
	}

	@Override
	public ClientSession getSession(ClientSessionOptions options, ReactiveTransactionAttemptContext atr) {
		// todo gp needed?
		return null;
//		ReactiveTransactionAttemptContext at = atr != null ? atr : AttemptContextReactiveAccessor.newAttemptContextReactive(transactions.reactive());
//
//		return new ClientSessionImpl(this, at);
	}

	@Override
	public ReactiveCouchbaseClientFactory withSession(ClientSession session) {
		return new ClientSessionBoundCouchbaseClientFactory(session, this);
	}

	@Override
	public boolean isTransactionActive() {
		return false;
	}

	//@Override
	//public CouchbaseStuffHandle getTransactionalOperator() {
	//	return transactionalOperator;
	//}

	//@Override
	//public ReactiveCouchbaseClientFactory with(CouchbaseStuffHandle txOp) {
	//	return new SimpleReactiveCouchbaseClientFactory((Cluster) getCluster().block(), bucketName, scopeName, txOp);
	//}

	private <T> T createProxyInstance(ClientSession session, T target, Class<T> targetType) {

		ProxyFactory factory = new ProxyFactory();
		factory.setTarget(target);
		factory.setInterfaces(targetType);
		factory.setOpaque(true);

		factory.addAdvice(new SessionAwareMethodInterceptor<>(session, target, ClientSession.class, ClusterInterface.class,
				this::proxyDatabase, Collection.class, this::proxyCollection));

		return targetType.cast(factory.getProxy(target.getClass().getClassLoader()));
	}

	private Collection proxyCollection(ClientSession session, Collection c) {
		return createProxyInstance(session, c, Collection.class);
	}

	private ClusterInterface proxyDatabase(ClientSession session, ClusterInterface cluster) {
		return createProxyInstance(session, cluster, ClusterInterface.class);
	}

	/**
	 * {@link ClientSession} bound TODO decorating the database with a {@link SessionAwareMethodInterceptor}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	static final class ClientSessionBoundCouchbaseClientFactory implements ReactiveCouchbaseClientFactory {

		private final ClientSession session;
		private final ReactiveCouchbaseClientFactory delegate;

		ClientSessionBoundCouchbaseClientFactory(ClientSession session, ReactiveCouchbaseClientFactory delegate) {
			this.session = session;
			this.delegate = delegate;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.ReactiveMongoDatabaseFactory#getMongoDatabase()
		 */
		@Override
		public Mono<ClusterInterface> getCluster() throws DataAccessException {
			return delegate.getCluster().map(this::decorateDatabase);
		}

		@Override
		public Mono<Bucket> getBucket() {
			return delegate.getBucket();
		}

		@Override
		public Mono<Scope> getScope() {
			return delegate.getScope();
		}

		@Override
		public Mono<Collection> getCollection(String name) {
			return delegate.getCollection(name);
		}

		@Override
		public Mono<Collection> getDefaultCollection() {
			return delegate.getDefaultCollection();
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
		 * @see org.springframework.data.mongodb.ReactiveMongoDatabaseFactory#getSession(com.mongodb.ClientSessionOptions)
		 */
		@Override
		public Mono<ClientSession> getSession(ClientSessionOptions options) {
			return Mono.just(getSession(options, null));
		}

		@Override
		public ClientSession getSession(ClientSessionOptions options, ReactiveTransactionAttemptContext atr) {
			return delegate.getSession(options, atr);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.ReactiveMongoDatabaseFactory#withSession(com.mongodb.session.ClientSession)
		 */
		@Override
		public ReactiveCouchbaseClientFactory withSession(ClientSession session) {
			return delegate.withSession(session);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.ReactiveMongoDatabaseFactory#isTransactionActive()
		 */
		@Override
		public boolean isTransactionActive() {
			return session != null && session.hasActiveTransaction();
		}

		//@Override
		//public CouchbaseStuffHandle getTransactionalOperator() {
		//	return delegate.getTransactionalOperator();
		//}

		//@Override
		//public ReactiveCouchbaseClientFactory with(CouchbaseStuffHandle txOp) {
		//	return delegate.with(txOp);
		//}

		private ClusterInterface decorateDatabase(ClusterInterface database) {
			return createProxyInstance(session, database, ClusterInterface.class);
		}

		private ClusterInterface proxyDatabase(ClientSession session, ClusterInterface database) {
			return createProxyInstance(session, database, ClusterInterface.class);
		}

		private Collection proxyCollection(ClientSession session, Collection collection) {
			return createProxyInstance(session, collection, Collection.class);
		}

		private <T> T createProxyInstance(ClientSession session, T target, Class<T> targetType) {

			ProxyFactory factory = new ProxyFactory();
			factory.setTarget(target);
			factory.setInterfaces(targetType);
			factory.setOpaque(true);

			factory.addAdvice(new SessionAwareMethodInterceptor<>(session, target, ClientSession.class,
					ClusterInterface.class, this::proxyDatabase, Collection.class, this::proxyCollection));

			return targetType.cast(factory.getProxy(target.getClass().getClassLoader()));
		}

		public ClientSession getSession() {
			return this.session;
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

			ClientSessionBoundCouchbaseClientFactory that = (ClientSessionBoundCouchbaseClientFactory) o;

			if (!ObjectUtils.nullSafeEquals(this.session, that.session)) {
				return false;
			}
			return ObjectUtils.nullSafeEquals(this.delegate, that.delegate);
		}

		@Override
		public int hashCode() {
			int result = ObjectUtils.nullSafeHashCode(this.session);
			result = 31 * result + ObjectUtils.nullSafeHashCode(this.delegate);
			return result;
		}

		public String toString() {
			return "SimpleReactiveCouchbaseDatabaseFactory.ClientSessionBoundCouchDbFactory(session=" + this.getSession()
					+ ", delegate=" + this.getDelegate() + ")";
		}
	}
}
