/*
 * Copyright 2019-2021 the original author or authors.
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
package org.springframework.data.couchbase.transaction;

import com.couchbase.client.java.transactions.config.TransactionOptions;
import reactor.core.publisher.Mono;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.ReactiveCouchbaseClientFactory;
import org.springframework.lang.Nullable;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.reactive.AbstractReactiveTransactionManager;
import org.springframework.transaction.reactive.GenericReactiveTransaction;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.springframework.transaction.support.SmartTransactionObject;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.java.transactions.Transactions;

/**
 * A {@link org.springframework.transaction.ReactiveTransactionManager} implementation that manages
 * {@link CoreTransactionAttemptContext} based transactions for a single {@link CouchbaseClientFactory}.
 * <p />
 * Binds a {@link CoreTransactionAttemptContext} from the specified {@link CouchbaseClientFactory} to the subscriber
 * {@link reactor.util.context.Context}.
 * <p />
 * {@link org.springframework.transaction.TransactionDefinition#isReadOnly() Readonly} transactions operate on a
 * {@link CoreTransactionAttemptContext} and enable causal consistency, and also
 * {@link CoreTransactionAttemptContext#startTransaction() start},
 * {@link CoreTransactionAttemptContext#commitTransaction() commit} or
 * {@link CoreTransactionAttemptContext#abortTransaction() abort} a transaction.
 * <p />
 * Application code is required to retrieve the {link com.xxxxxxx.reactivestreams.client.MongoDatabase} via {link
 * org.springframework.data.xxxxxxx.ReactiveMongoDatabaseUtils#getDatabase(CouchbaseClientFactory)} instead of a
 * standard {@link org.springframework.data.couchbase.CouchbaseClientFactory#getCluster()} call. Spring classes such as
 * {@link org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate} use this strategy implicitly.
 * <p />
 * By default failure of a {@literal commit} operation raises a {@link TransactionSystemException}. You can override
 * {@link #doCommit(TransactionSynchronizationManager, ReactiveCouchbaseTransactionObject)} to implement the
 * <a href="https://docs.xxxxxxx.com/manual/core/transactions/#retry-commit-operation">Retry Commit Operation</a>
 * behavior as outlined in the XxxxxxXX reference manual.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.2
 * @see <a href="https://www.xxxxxxx.com/transactions">XxxxxxXX Transaction Documentation</a> see
 *      ReactiveMongoDatabaseUtils#getDatabase(CouchbaseClientFactory, SessionSynchronization)
 */
public class ReactiveCouchbaseTransactionManager extends AbstractReactiveTransactionManager
		implements InitializingBean {

	private @Nullable ReactiveCouchbaseClientFactory databaseFactory; // (why) does this need to be reactive?
	private @Nullable Transactions transactions;

	/**
	 * Create a new {@link ReactiveCouchbaseTransactionManager} for bean-style usage.
	 * <p />
	 * <strong>Note:</strong>The {@link org.springframework.data.couchbase.CouchbaseClientFactory db factory} has to be
	 * {@link #setDatabaseFactory(ReactiveCouchbaseClientFactory)} set} before using the instance. Use this constructor to
	 * prepare a {@link ReactiveCouchbaseTransactionManager} via a {@link org.springframework.beans.factory.BeanFactory}.
	 * <p />
	 * Optionally it is possible to set default {@link TransactionQueryOptions transaction options} defining {link
	 * com.xxxxxxx.ReadConcern} and {link com.xxxxxxx.WriteConcern}.
	 *
	 * @see #setDatabaseFactory(ReactiveCouchbaseClientFactory)
	 */
	public ReactiveCouchbaseTransactionManager() {}

	/**
	 * Create a new {@link ReactiveCouchbaseTransactionManager} obtaining sessions from the given
	 * {@link CouchbaseClientFactory} applying the given {@link TransactionQueryOptions options}, if present, when
	 * starting a new transaction.
	 *
	 * @param databaseFactory must not be {@literal null}.
	 */
	public ReactiveCouchbaseTransactionManager(ReactiveCouchbaseClientFactory databaseFactory) {
		Assert.notNull(databaseFactory, "DatabaseFactory must not be null!");
		this.databaseFactory = databaseFactory; // should be a clone? TransactionSynchronizationManager binds objs to it
		System.err.println("ReactiveCouchbaseTransactionManager : created");
	}

	public ReactiveCouchbaseTransactionManager(ReactiveCouchbaseClientFactory databaseFactory,
			@Nullable Transactions transactions) {
		Assert.notNull(databaseFactory, "DatabaseFactory must not be null!");
		this.databaseFactory = databaseFactory; // databaseFactory; // should be a clone? TransactionSynchronizationManager
																						// binds objs to it
		this.transactions = transactions;
		System.err.println("ReactiveCouchbaseTransactionManager : created Transactions: " + transactions);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.transaction.reactive.AbstractReactiveTransactionManager#doGetTransaction(org.springframework.transaction.reactive.TransactionSynchronizationManager)
	 */
	@Override
	protected Object doGetTransaction(TransactionSynchronizationManager synchronizationManager)
			throws TransactionException {
		// creation of a new ReactiveCouchbaseTransactionObject (i.e. transaction).
		// with an attempt to get the resourceHolder from the synchronizationManager
		ReactiveCouchbaseResourceHolder resourceHolder = (ReactiveCouchbaseResourceHolder) synchronizationManager
				.getResource(getRequiredDatabaseFactory().getCluster().block());
		// TODO ACR from couchbase
		// resourceHolder.getSession().setAttemptContextReactive(null);
		return new ReactiveCouchbaseTransactionObject(resourceHolder);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.transaction.reactive.AbstractReactiveTransactionManager#isExistingTransaction(java.lang.Object)
	 */
	@Override
	protected boolean isExistingTransaction(Object transaction) throws TransactionException {
		return extractCouchbaseTransaction(transaction).hasResourceHolder();
	}

	/**
	 * doBegin() attaches the atr from the transactionOperator in the transactionDefinition to the transaction (via
	 * resourceHolder -> Clientsession) (non-Javadoc)
	 *
	 * @see org.springframework.transaction.reactive.AbstractReactiveTransactionManager#doBegin(org.springframework.transaction.reactive.TransactionSynchronizationManager,
	 *      java.lang.Object, org.springframework.transaction.TransactionDefinition)
	 */
	@Override
	protected Mono<Void> doBegin(TransactionSynchronizationManager synchronizationManager, Object transaction,
			TransactionDefinition definition) throws TransactionException {

		return Mono.defer(() -> {

			ReactiveCouchbaseTransactionObject couchbaseTransactionObject = extractCouchbaseTransaction(transaction);
			// TODO mr - why aren't we creating the AttemptContext here in the client session in the resourceholder?
			Mono<ReactiveCouchbaseResourceHolder> holder = newResourceHolder(definition,
					TransactionOptions.transactionOptions());
			return holder.doOnNext(resourceHolder -> {
				couchbaseTransactionObject.setResourceHolder(resourceHolder);

				if (logger.isDebugEnabled()) {
					logger.debug(
							String.format("About to start transaction for session %s.", debugString(resourceHolder.getCore())));
				}

			}).doOnNext(resourceHolder -> {

				couchbaseTransactionObject.startTransaction();

				if (logger.isDebugEnabled()) {
					logger.debug(String.format("Started transaction for session %s.", debugString(resourceHolder.getCore())));
				}

			})//
					.onErrorMap(ex -> new TransactionSystemException(
							String.format("Could not start Couchbase transaction for session %s.",
									debugString(couchbaseTransactionObject.getCore())),
							ex))
					.doOnSuccess(resourceHolder -> {
						System.err.println("ReactiveCouchbaseTransactionManager: " + this);
						System.err.println(
								"bindResource: " + getRequiredDatabaseFactory().getCluster().block() + " value: " + resourceHolder);
						synchronizationManager.bindResource(getRequiredDatabaseFactory().getCluster().block(), resourceHolder);
					}).then();
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.transaction.reactive.AbstractReactiveTransactionManager#doSuspend(org.springframework.transaction.reactive.TransactionSynchronizationManager, java.lang.Object)
	 */
	@Override
	protected Mono<Object> doSuspend(TransactionSynchronizationManager synchronizationManager, Object transaction)
			throws TransactionException {

		return Mono.fromSupplier(() -> {

			ReactiveCouchbaseTransactionObject mongoTransactionObject = extractCouchbaseTransaction(transaction);
			mongoTransactionObject.setResourceHolder(null);

			return synchronizationManager.unbindResource(getRequiredDatabaseFactory());
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.transaction.reactive.AbstractReactiveTransactionManager#doResume(org.springframework.transaction.reactive.TransactionSynchronizationManager, java.lang.Object, java.lang.Object)
	 */
	@Override
	protected Mono<Void> doResume(TransactionSynchronizationManager synchronizationManager, @Nullable Object transaction,
			Object suspendedResources) {
		return Mono
				.fromRunnable(() -> synchronizationManager.bindResource(getRequiredDatabaseFactory(), suspendedResources));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.transaction.reactive.AbstractReactiveTransactionManager#doCommit(org.springframework.transaction.reactive.TransactionSynchronizationManager, org.springframework.transaction.reactive.GenericReactiveTransaction)
	 */
	@Override
	protected final Mono<Void> doCommit(TransactionSynchronizationManager synchronizationManager,
			GenericReactiveTransaction status) throws TransactionException {
		return Mono.defer(() -> {

			ReactiveCouchbaseTransactionObject couchbaseTransactionObject = extractCouchbaseTransaction(status);

			if (logger.isDebugEnabled()) {
				logger.debug(String.format("About to doCommit transaction for session %s.",
						debugString(couchbaseTransactionObject.getCore())));
			}

			return doCommit(synchronizationManager, couchbaseTransactionObject).onErrorMap(ex -> {
				return new TransactionSystemException(String.format("Could not commit Couchbase transaction for session %s.",
						debugString(couchbaseTransactionObject.getCore())), ex);
			});
		});
	}

	/**
	 * Customization hook to perform an actual commit of the given transaction.<br />
	 * If a commit operation encounters an error, the XxxxxxXX driver throws a {@link CouchbaseException} holding
	 * {@literal error labels}. <br />
	 * By default those labels are ignored, nevertheless one might check for {@link CouchbaseException transient commit
	 * errors labels} and retry the the commit.
	 *
	 * @param synchronizationManager reactive synchronization manager.
	 * @param transactionObject never {@literal null}.
	 */
	protected Mono<Void> doCommit(TransactionSynchronizationManager synchronizationManager,
			ReactiveCouchbaseTransactionObject transactionObject) {
		return transactionObject.commitTransaction();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.transaction.reactive.AbstractReactiveTransactionManager#doRollback(org.springframework.transaction.reactive.TransactionSynchronizationManager, org.springframework.transaction.reactive.GenericReactiveTransaction)
	 */
	@Override
	protected Mono<Void> doRollback(TransactionSynchronizationManager synchronizationManager,
			GenericReactiveTransaction status) {

		return Mono.defer(() -> {

			ReactiveCouchbaseTransactionObject couchbaseTransactionObject = extractCouchbaseTransaction(status);

			if (logger.isDebugEnabled()) {
				logger.debug(String.format("About to abort transaction for session %s.",
						debugString(couchbaseTransactionObject.getCore())));
			}

			return couchbaseTransactionObject.abortTransaction().onErrorResume(CouchbaseException.class, ex -> {
				return Mono.error(new TransactionSystemException(String.format("Could not abort transaction for session %s.",
						debugString(couchbaseTransactionObject.getCore())), ex));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.transaction.reactive.AbstractReactiveTransactionManager#doSetRollbackOnly(org.springframework.transaction.reactive.TransactionSynchronizationManager, org.springframework.transaction.reactive.GenericReactiveTransaction)
	 */
	@Override
	protected Mono<Void> doSetRollbackOnly(TransactionSynchronizationManager synchronizationManager,
			GenericReactiveTransaction status) throws TransactionException {

		return Mono.fromRunnable(() -> {
			ReactiveCouchbaseTransactionObject transactionObject = extractCouchbaseTransaction(status);
			transactionObject.getRequiredResourceHolder().setRollbackOnly();
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.transaction.reactive.AbstractReactiveTransactionManager#doCleanupAfterCompletion(org.springframework.transaction.reactive.TransactionSynchronizationManager, java.lang.Object)
	 */
	@Override
	protected Mono<Void> doCleanupAfterCompletion(TransactionSynchronizationManager synchronizationManager,
			Object transaction) {

		Assert.isInstanceOf(ReactiveCouchbaseTransactionObject.class, transaction,
				() -> String.format("Expected to find a %s but it turned out to be %s.",
						ReactiveCouchbaseTransactionObject.class, transaction.getClass()));

		return Mono.fromRunnable(() -> {
			ReactiveCouchbaseTransactionObject couchbaseTransactionObject = (ReactiveCouchbaseTransactionObject) transaction;

			// Remove the connection holder from the thread.
			synchronizationManager.unbindResource(getRequiredDatabaseFactory().getCluster().block());
			couchbaseTransactionObject.getRequiredResourceHolder().clear();

			if (logger.isDebugEnabled()) {
				logger.debug(String.format("About to release Session %s after transaction.",
						debugString(couchbaseTransactionObject.getCore())));
			}

			couchbaseTransactionObject.closeSession();
		});
	}

	/**
	 * Set the {@link CouchbaseClientFactory} that this instance should manage transactions for.
	 *
	 * @param databaseFactory must not be {@literal null}.
	 */
	public void setDatabaseFactory(ReactiveCouchbaseClientFactory databaseFactory) {

		Assert.notNull(databaseFactory, "DatabaseFactory must not be null!");
		this.databaseFactory = databaseFactory;
	}

	/**
	 * Get the {@link CouchbaseClientFactory} that this instance manages transactions for.
	 *
	 * @return can be {@literal null}.
	 */
	@Nullable
	public ReactiveCouchbaseClientFactory getDatabaseFactory() {
		return databaseFactory;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() {
		getRequiredDatabaseFactory();
	}

	private Mono<ReactiveCouchbaseResourceHolder> newResourceHolder(TransactionDefinition definition,
			TransactionOptions options) {

		ReactiveCouchbaseClientFactory dbFactory = getRequiredDatabaseFactory();
		// TODO MSR : config should be derived from config that was used for `transactions`
		Mono<ReactiveCouchbaseResourceHolder> sess = Mono.just(dbFactory.getTransactionResources(options, null));
		return sess;
	}

	/**
	 * @throws IllegalStateException if {@link #databaseFactory} is {@literal null}.
	 */
	private ReactiveCouchbaseClientFactory getRequiredDatabaseFactory() {
		Assert.state(databaseFactory != null,
				"ReactiveCouchbaseTransactionManager operates upon a CouchbaseClientFactory. Did you forget to provide one? It's required.");
		return databaseFactory;
	}

	private static ReactiveCouchbaseTransactionObject extractCouchbaseTransaction(Object transaction) {

		Assert.isInstanceOf(ReactiveCouchbaseTransactionObject.class, transaction,
				() -> String.format("Expected to find a %s but it turned out to be %s.",
						ReactiveCouchbaseTransactionObject.class, transaction.getClass()));

		return (ReactiveCouchbaseTransactionObject) transaction;
	}

	private static ReactiveCouchbaseTransactionObject extractCouchbaseTransaction(GenericReactiveTransaction status) {

		Assert.isInstanceOf(ReactiveCouchbaseTransactionObject.class, status.getTransaction(),
				() -> String.format("Expected to find a %s but it turned out to be %s.",
						ReactiveCouchbaseTransactionObject.class, status.getTransaction().getClass()));

		return (ReactiveCouchbaseTransactionObject) status.getTransaction();
	}

	private static String debugString(@Nullable CoreTransactionAttemptContext session) {

		if (session == null) {
			return "null";
		}

		String debugString = String.format("[%s@%s ", ClassUtils.getShortName(session.getClass()),
				Integer.toHexString(session.hashCode()));

		try {
			debugString += String.format("core=%s", session);
		} catch (RuntimeException e) {
			debugString += String.format("error = %s", e.getMessage());
		}

		debugString += "]";

		return debugString;
	}

	/**
	 * Couchbase specific transaction object, representing a {@link ReactiveCouchbaseResourceHolder}. Used as transaction
	 * object by {@link ReactiveCouchbaseTransactionManager}.
	 *
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 * @since 2.2
	 * @see ReactiveCouchbaseResourceHolder
	 */
	protected static class ReactiveCouchbaseTransactionObject implements SmartTransactionObject {

		public @Nullable ReactiveCouchbaseResourceHolder resourceHolder;

		ReactiveCouchbaseTransactionObject(@Nullable ReactiveCouchbaseResourceHolder resourceHolder) {
			this.resourceHolder = resourceHolder;
		}

		/**
		 * Set the {@link ReactiveCouchbaseResourceHolder}.
		 *
		 * @param resourceHolder can be {@literal null}.
		 */
		void setResourceHolder(@Nullable ReactiveCouchbaseResourceHolder resourceHolder) {
			this.resourceHolder = resourceHolder;
		}

		/**
		 * @return {@literal true} if a {@link ReactiveCouchbaseResourceHolder} is set.
		 */
		final boolean hasResourceHolder() {
			return resourceHolder != null;
		}

		/**
		 * Start a XxxxxxXX transaction optionally given {@link TransactionQueryOptions}. todo gp how to expose
		 * TransactionOptions
		 * 
		 * @param options can be {@literal null}
		 */
		void startTransaction() {

			CoreTransactionAttemptContext core = getRequiredCore();
			// core.startTransaction();
		}

		/**
		 * Commit the transaction.
		 */
		public Mono<Void> commitTransaction() {
			return getRequiredCore().commit();
		}

		/**
		 * Rollback (abort) the transaction.
		 */
		public Mono<Void> abortTransaction() {
			return getRequiredCore().rollback();
		}

		/**
		 * Close a {@link CoreTransactionAttemptContext} without regard to its transactional state.
		 */
		void closeSession() {
			CoreTransactionAttemptContext session = getRequiredCore();
			// if (session.getServerSession() != null && !session.getServerSession().isClosed()) {
			// session.close();
			// }
		}

		@Nullable
		public CoreTransactionAttemptContext getCore() {
			return resourceHolder != null ? resourceHolder.getCore() : null;
		}

		private ReactiveCouchbaseResourceHolder getRequiredResourceHolder() {

			Assert.state(resourceHolder != null, "ReactiveMongoResourceHolder is required but not present. o_O");
			return resourceHolder;
		}

		private CoreTransactionAttemptContext getRequiredCore() {
			CoreTransactionAttemptContext core = getCore();
			Assert.state(core != null, "A CoreTransactionAttemptContext is required but it turned out to be null.");
			return core;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.transaction.support.SmartTransactionObject#isRollbackOnly()
		 */
		@Override
		public boolean isRollbackOnly() {
			return this.resourceHolder != null && this.resourceHolder.isRollbackOnly();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.transaction.support.SmartTransactionObject#flush()
		 */
		@Override
		public void flush() {
			throw new UnsupportedOperationException("flush() not supported");
		}
	}
}
