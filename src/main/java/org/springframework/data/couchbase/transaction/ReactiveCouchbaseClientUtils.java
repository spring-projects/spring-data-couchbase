package org.springframework.data.couchbase.transaction;

import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.java.ClusterInterface;
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
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

public class ReactiveCouchbaseClientUtils {

	public static Mono<ReactiveCouchbaseTemplate> getTemplate(ReactiveCouchbaseClientFactory factory,
															  SessionSynchronization sessionSynchronization, CouchbaseConverter converter) {
		return doGetCouchbaseTemplate(null, factory, sessionSynchronization, converter);
	}

	private static Mono<ReactiveCouchbaseTemplate> doGetCouchbaseTemplate(@Nullable String dbName,
																		  ReactiveCouchbaseClientFactory factory, SessionSynchronization sessionSynchronization,
																		  CouchbaseConverter converter) {

		Assert.notNull(factory, "DatabaseFactory must not be null!");

		if (sessionSynchronization == SessionSynchronization.NEVER) {
			return getCouchbaseTemplateOrDefault(dbName, factory, converter);
		}

		return TransactionSynchronizationManager.forCurrentTransaction()
				.filter(TransactionSynchronizationManager::isSynchronizationActive) //
				.flatMap(synchronizationManager -> {
					return doGetSession(synchronizationManager, factory, sessionSynchronization) //
							.flatMap(it -> getCouchbaseTemplateOrDefault(dbName, factory.withResources(it), converter)); // rx TxMgr
				}) //
				.onErrorResume(NoTransactionException.class,
						e -> { return getCouchbaseTemplateOrDefault(dbName,
								getNonReactiveSession(factory) != null ? factory.withResources(getNonReactiveSession(factory)) : factory,
								converter);}) // blocking TxMgr
				.switchIfEmpty(getCouchbaseTemplateOrDefault(dbName, factory, converter));
	}

	private static ReactiveCouchbaseResourceHolder getNonReactiveSession(ReactiveCouchbaseClientFactory factory) {
		ReactiveCouchbaseResourceHolder h = ((ReactiveCouchbaseResourceHolder) org.springframework.transaction.support.TransactionSynchronizationManager
				.getResource(factory.getCluster()));
		if( h == null){  // no longer used
			h = ((ReactiveCouchbaseResourceHolder) org.springframework.transaction.support.TransactionSynchronizationManager
					.getResource(factory));// MN's CouchbaseTransactionManager
		}
		return h;
	}

	// TODO mr - unnecessary?
	private static Mono<ClusterInterface> getCouchbaseClusterOrDefault(@Nullable String dbName,
																	   ReactiveCouchbaseClientFactory factory) {
		return Mono.just(factory.getCluster());
	}

	private static Mono<ReactiveCouchbaseTemplate> getCouchbaseTemplateOrDefault(@Nullable String dbName,
																				 ReactiveCouchbaseClientFactory factory, CouchbaseConverter converter) {
		return Mono.just(new ReactiveCouchbaseTemplate(factory, converter));
	}

	private static Mono<ReactiveCouchbaseResourceHolder> doGetSession(TransactionSynchronizationManager synchronizationManager,
																	  ReactiveCouchbaseClientFactory dbFactory, SessionSynchronization sessionSynchronization) {

		final ReactiveCouchbaseResourceHolder registeredHolder = (ReactiveCouchbaseResourceHolder) synchronizationManager
				.getResource(dbFactory.getCluster()); // make sure this wasn't saved under the wrong key!!!

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
		// todo gp but this always returns null - does this code get executed anywhere?
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
