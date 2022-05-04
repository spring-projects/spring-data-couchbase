package org.springframework.data.couchbase.transaction;

import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.java.ClusterInterface;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import org.springframework.data.couchbase.ReactiveCouchbaseClientFactory;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.lang.Nullable;
import org.springframework.transaction.NoTransactionException;
import org.springframework.transaction.reactive.ReactiveResourceSynchronization;
import org.springframework.transaction.reactive.TransactionSynchronization;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.springframework.transaction.support.ResourceHolderSynchronization;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

public class ReactiveCouchbaseClientUtils {

	/**
	 * Check if the {@link ReactiveMongoDatabaseFactory} is actually bound to a
	 * {@link com.mongodb.reactivestreams.client.ClientSession} that has an active transaction, or if a
	 * {@link org.springframework.transaction.reactive.TransactionSynchronization} has been registered for the
	 * {@link ReactiveMongoDatabaseFactory resource} and if the associated
	 * {@link com.mongodb.reactivestreams.client.ClientSession} has an
	 * {@link com.mongodb.reactivestreams.client.ClientSession#hasActiveTransaction() active transaction}.
	 *
	 * @param databaseFactory the resource to check transactions for. Must not be {@literal null}.
	 * @return a {@link Mono} emitting {@literal true} if the factory has an ongoing transaction.
	 */
	public static Mono<Boolean> isTransactionActive(ReactiveCouchbaseClientFactory databaseFactory) {

		if (databaseFactory.isTransactionActive()) {
			return Mono.just(true);
		}

		return TransactionSynchronizationManager.forCurrentTransaction() //
				.map(it -> {

					ReactiveCouchbaseResourceHolder holder = (ReactiveCouchbaseResourceHolder) it.getResource(databaseFactory);
					return holder != null && holder.hasActiveTransaction();
				}) //
				.onErrorResume(NoTransactionException.class, e -> Mono.just(false));
	}

	/**
	 * Obtain the default {@link MongoDatabase database} form the given {@link ReactiveMongoDatabaseFactory factory} using
	 * {@link SessionSynchronization#ON_ACTUAL_TRANSACTION native session synchronization}. <br />
	 * Registers a {@link MongoSessionSynchronization MongoDB specific transaction synchronization} within the subscriber
	 * {@link Context} if {@link TransactionSynchronizationManager#isSynchronizationActive() synchronization is active}.
	 *
	 * @param factory the {@link ReactiveMongoDatabaseFactory} to get the {@link MongoDatabase} from.
	 * @return the {@link MongoDatabase} that is potentially associated with a transactional {@link ClientSession}.
	 */
	public static Mono<ClusterInterface> getDatabase(ReactiveCouchbaseClientFactory factory) {
		return doGetCouchbaseCluster(null, factory, SessionSynchronization.ON_ACTUAL_TRANSACTION);
	}

	/**
	 * Obtain the default {@link MongoDatabase database} form the given {@link ReactiveMongoDatabaseFactory factory}.
	 * <br />
	 * Registers a {@link MongoSessionSynchronization MongoDB specific transaction synchronization} within the subscriber
	 * {@link Context} if {@link TransactionSynchronizationManager#isSynchronizationActive() synchronization is active}.
	 *
	 * @param factory the {@link ReactiveMongoDatabaseFactory} to get the {@link MongoDatabase} from.
	 * @param sessionSynchronization the synchronization to use. Must not be {@literal null}.
	 * @return the {@link MongoDatabase} that is potentially associated with a transactional {@link ClientSession}.
	 */
	public static Mono<ClusterInterface> getDatabase(ReactiveCouchbaseClientFactory factory,
			SessionSynchronization sessionSynchronization) {
		return doGetCouchbaseCluster(null, factory, sessionSynchronization);
	}

	public static Mono<ReactiveCouchbaseTemplate> getTemplate(ReactiveCouchbaseClientFactory factory,
			SessionSynchronization sessionSynchronization, CouchbaseConverter converter) {
		return doGetCouchbaseTemplate(null, factory, sessionSynchronization, converter);
	}

	/**
	 * Obtain the {@link MongoDatabase database} with given name form the given {@link ReactiveMongoDatabaseFactory
	 * factory} using {@link SessionSynchronization#ON_ACTUAL_TRANSACTION native session synchronization}. <br />
	 * Registers a {@link MongoSessionSynchronization MongoDB specific transaction synchronization} within the subscriber
	 * {@link Context} if {@link TransactionSynchronizationManager#isSynchronizationActive() synchronization is active}.
	 *
	 * @param dbName the name of the {@link MongoDatabase} to get.
	 * @param factory the {@link ReactiveMongoDatabaseFactory} to get the {@link MongoDatabase} from.
	 * @return the {@link MongoDatabase} that is potentially associated with a transactional {@link ClientSession}.
	 */
	public static Mono<ClusterInterface> getDatabase(String dbName, ReactiveCouchbaseClientFactory factory) {
		return doGetCouchbaseCluster(dbName, factory, SessionSynchronization.ON_ACTUAL_TRANSACTION);
	}

	/**
	 * Obtain the {@link MongoDatabase database} with given name form the given {@link ReactiveMongoDatabaseFactory
	 * factory}. <br />
	 * Registers a {@link MongoSessionSynchronization MongoDB specific transaction synchronization} within the subscriber
	 * {@link Context} if {@link TransactionSynchronizationManager#isSynchronizationActive() synchronization is active}.
	 *
	 * @param dbName the name of the {@link MongoDatabase} to get.
	 * @param factory the {@link ReactiveMongoDatabaseFactory} to get the {@link MongoDatabase} from.
	 * @param sessionSynchronization the synchronization to use. Must not be {@literal null}.
	 * @return the {@link MongoDatabase} that is potentially associated with a transactional {@link ClientSession}.
	 */
	public static Mono<ClusterInterface> getCluster(String dbName, ReactiveCouchbaseClientFactory factory,
			SessionSynchronization sessionSynchronization) {
		return doGetCouchbaseCluster(dbName, factory, sessionSynchronization);
	}

	private static Mono<ClusterInterface> doGetCouchbaseCluster(@Nullable String dbName,
			ReactiveCouchbaseClientFactory factory, SessionSynchronization sessionSynchronization) {

		Assert.notNull(factory, "DatabaseFactory must not be null!");

		if (sessionSynchronization == SessionSynchronization.NEVER) {
			return getCouchbaseClusterOrDefault(dbName, factory);
		}

		return TransactionSynchronizationManager.forCurrentTransaction()
				.filter(TransactionSynchronizationManager::isSynchronizationActive) //
				.flatMap(synchronizationManager -> {

					return doGetSession(synchronizationManager, factory, sessionSynchronization) //
							.flatMap(it -> getCouchbaseClusterOrDefault(dbName, factory.withCore(it)));
				}) //
				.onErrorResume(NoTransactionException.class, e -> getCouchbaseClusterOrDefault(dbName, factory)) // hitting this
				.switchIfEmpty(getCouchbaseClusterOrDefault(dbName, factory));
	}

	private static Mono<ReactiveCouchbaseTemplate> doGetCouchbaseTemplate(@Nullable String dbName,
			ReactiveCouchbaseClientFactory factory, SessionSynchronization sessionSynchronization,
			CouchbaseConverter converter) {

		Assert.notNull(factory, "DatabaseFactory must not be null!");

		if (sessionSynchronization == SessionSynchronization.NEVER) {
			return getCouchbaseTemplateOrDefault(dbName, factory, converter);
		}

		//CouchbaseResourceHolder h = (CouchbaseResourceHolder) org.springframework.transaction.support.TransactionSynchronizationManager
		//		.getResource(factory);

		return TransactionSynchronizationManager.forCurrentTransaction()
				.filter(TransactionSynchronizationManager::isSynchronizationActive) //
				.flatMap(synchronizationManager -> {
					return doGetSession(synchronizationManager, factory, sessionSynchronization) //
							.flatMap(it -> getCouchbaseTemplateOrDefault(dbName, factory.withCore(it), converter)); // rx TxMgr
				}) //
				.onErrorResume(NoTransactionException.class,
						e -> { return getCouchbaseTemplateOrDefault(dbName,
								getNonReactiveSession(factory) != null ? factory.withCore(getNonReactiveSession(factory)) : factory,
								converter);}) // blocking TxMgr
				.switchIfEmpty(getCouchbaseTemplateOrDefault(dbName, factory, converter));
	}

	private static ReactiveCouchbaseResourceHolder getNonReactiveSession(ReactiveCouchbaseClientFactory factory) {
		ReactiveCouchbaseResourceHolder h = ((ReactiveCouchbaseResourceHolder) org.springframework.transaction.support.TransactionSynchronizationManager
				.getResource(factory.getCluster().block()));
		if( h == null){  // no longer used
			h = ((ReactiveCouchbaseResourceHolder) org.springframework.transaction.support.TransactionSynchronizationManager
					.getResource(factory));// MN's CouchbaseTransactionManager
		}
		//System.err.println("getNonreactiveSession: "+ h);
		return h;
	}

	private static Mono<ClusterInterface> getCouchbaseClusterOrDefault(@Nullable String dbName,
			ReactiveCouchbaseClientFactory factory) {
		return StringUtils.hasText(dbName) ? factory.getCluster() : factory.getCluster();
	}

	private static Mono<ReactiveCouchbaseTemplate> getCouchbaseTemplateOrDefault(@Nullable String dbName,
			ReactiveCouchbaseClientFactory factory, CouchbaseConverter converter) {
		return Mono.just(new ReactiveCouchbaseTemplate(factory, converter));
	}

	private static Mono<ReactiveCouchbaseResourceHolder> doGetSession(TransactionSynchronizationManager synchronizationManager,
			ReactiveCouchbaseClientFactory dbFactory, SessionSynchronization sessionSynchronization) {

		final ReactiveCouchbaseResourceHolder registeredHolder = (ReactiveCouchbaseResourceHolder) synchronizationManager
				.getResource(dbFactory.getCluster().block()); // make sure this wasn't saved under the wrong key!!!

		// check for native MongoDB transaction
		if (registeredHolder != null
				&& (registeredHolder.hasCore() || registeredHolder.isSynchronizedWithTransaction())) {
			System.err.println("doGetSession: got: "+registeredHolder.getCore());
			// TODO msr - mabye don't create a session unless it has an atr?
			//return registeredHolder.hasCore() ? Mono.just(registeredHolder)
			//		: createClientSession(dbFactory).map( core -> { registeredHolder.setCore(core); return registeredHolder;});
			return Mono.just(registeredHolder);
		}

		if (SessionSynchronization.ON_ACTUAL_TRANSACTION.equals(sessionSynchronization)) {
			System.err.println("doGetSession: ON_ACTUAL_TRANSACTION -> empty()");
			return Mono.empty();
		}

		System.err.println("doGetSession: createClientSession()");

		// init a non native MongoDB transaction by registering a MongoSessionSynchronization
		return createClientSession(dbFactory).map(session -> {

			ReactiveCouchbaseResourceHolder newHolder = new ReactiveCouchbaseResourceHolder(session);
			//newHolder.getRequiredCore().startTransaction();
			System.err.println(" need to call startTransaction() ");

			synchronizationManager
					.registerSynchronization(new CouchbaseSessionSynchronization(synchronizationManager, newHolder, dbFactory));
			newHolder.setSynchronizedWithTransaction(true);
			synchronizationManager.bindResource(dbFactory, newHolder);

			return newHolder;
		});
	}

	private static Mono<CoreTransactionAttemptContext> createClientSession(ReactiveCouchbaseClientFactory dbFactory) {
		return null; // ?? dbFactory.getCore(TransactionOptions.transactionOptions());
	}

	/**
	 * MongoDB specific {@link ResourceHolderSynchronization} for resource cleanup at the end of a transaction when
	 * participating in a non-native MongoDB transaction, such as a R2CBC transaction.
	 *
	 * @author Mark Paluch
	 * @since 2.2
	 */
	private static class CouchbaseSessionSynchronization
			extends ReactiveResourceSynchronization<ReactiveCouchbaseResourceHolder, Object> {

		private final ReactiveCouchbaseResourceHolder resourceHolder;

		CouchbaseSessionSynchronization(TransactionSynchronizationManager synchronizationManager,
				ReactiveCouchbaseResourceHolder resourceHolder, ReactiveCouchbaseClientFactory dbFactory) {

			super(resourceHolder, dbFactory, synchronizationManager);
			this.resourceHolder = resourceHolder;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.transaction.reactive.ReactiveResourceSynchronization#shouldReleaseBeforeCompletion()
		 */
		@Override
		protected boolean shouldReleaseBeforeCompletion() {
			return false;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.transaction.reactive.ReactiveResourceSynchronization#processResourceAfterCommit(java.lang.Object)
		 */
		@Override
		protected Mono<Void> processResourceAfterCommit(ReactiveCouchbaseResourceHolder resourceHolder) {

			if (isTransactionActive(resourceHolder)) {
				return Mono.from(resourceHolder.getCore().commit());
			}

			return Mono.empty();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.transaction.reactive.ReactiveResourceSynchronization#afterCompletion(int)
		 */
		@Override
		public Mono<Void> afterCompletion(int status) {

			return Mono.defer(() -> {

				if (status == TransactionSynchronization.STATUS_ROLLED_BACK && isTransactionActive(this.resourceHolder)) {

					return Mono.from(resourceHolder.getCore().rollback()) //
							.then(super.afterCompletion(status));
				}

				return super.afterCompletion(status);
			});
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.transaction.reactive.ReactiveResourceSynchronization#releaseResource(java.lang.Object, java.lang.Object)
		 */
		@Override
		protected Mono<Void> releaseResource(ReactiveCouchbaseResourceHolder resourceHolder, Object resourceKey) {

			return Mono.fromRunnable(() -> {
				//if (resourceHolder.hasActiveSession()) {
				//	resourceHolder.getRequiredSession().close();
				//}
			});
		}

		private boolean isTransactionActive(ReactiveCouchbaseResourceHolder resourceHolder) {

			if (!resourceHolder.hasCore()) {
				return false;
			}

			return resourceHolder.getRequiredCore() != null;
		}
	}
}
