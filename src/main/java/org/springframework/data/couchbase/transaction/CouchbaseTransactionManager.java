/*
 * Copyright 2018-2021 the original author or authors.
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

import com.couchbase.client.java.transactions.ReactiveTransactionAttemptContext;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.lang.Nullable;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.ResourceTransactionManager;
import org.springframework.transaction.support.SmartTransactionObject;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionSynchronizationUtils;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.couchbase.client.core.error.CouchbaseException;
import reactor.core.publisher.Mono;

/**
 * A {@link org.springframework.transaction.PlatformTransactionManager} implementation that manages
 * {@link ClientSession} based transactions for a single {@link CouchbaseClientFactory}.
 * <p />
 * Binds a {@link ClientSession} from the specified {@link CouchbaseClientFactory} to the thread.
 * <p />
 * {@link TransactionDefinition#isReadOnly() Readonly} transactions operate on a {@link ClientSession} and enable causal
 * consistency, and also {@link ClientSession#startTransaction() start}, {@link ClientSession#commitTransaction()
 * commit} or {@link ClientSession#abortTransaction() abort} a transaction.
 * <p />
 * TODO: Application code is required to retrieve the {@link com.couchbase.client.java.Cluster} ????? via
 * {@link ?????#getDatabase(CouchbaseClientFactory)} instead of a standard {@link CouchbaseClientFactory#getCluster()}
 * call. Spring classes such as {@link org.springframework.data.couchbase.core.CouchbaseTemplate} use this strategy
 * implicitly.
 * <p />
 * By default failure of a {@literal commit} operation raises a {@link TransactionSystemException}. One may override
 * {@link #doCommit(CouchbaseTransactionObject)} to implement the
 * <a href="https://docs.mongodb.com/manual/core/transactions/#retry-commit-operation">Retry Commit Operation</a>
 * behavior as outlined in the MongoDB reference manual.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 * @see <a href="https://www.mongodb.com/transactions">MongoDB Transaction Documentation</a>
 * @see MongoDatabaseUtils#getDatabase(CouchbaseClientFactory, SessionSynchronization)
 */
// todo gp is this needed, or can we only have the CallbackPreferring one?
public class CouchbaseTransactionManager extends AbstractPlatformTransactionManager
		implements ResourceTransactionManager, InitializingBean {

	private @Nullable CouchbaseClientFactory databaseFactory;
	private @Nullable TransactionOptions options;

	/**
	 * Create a new {@link CouchbaseTransactionManager} for bean-style usage.
	 * <p />
	 * <strong>Note:</strong>The {@link CouchbaseClientFactory db factory} has to be
	 * {@link #setDbFactory(CouchbaseClientFactory) set} before using the instance. Use this constructor to prepare a
	 * {@link CouchbaseTransactionManager} via a {@link org.springframework.beans.factory.BeanFactory}.
	 * <p />
	 * TODO: Optionally it is possible to set default {@link TransactionOptions transaction options} defining TODO:
	 * {@link ReadConcern} and {@link WriteConcern}.
	 *
	 * @see #setDbFactory(CouchbaseClientFactory)
	 * @see #setTransactionSynchronization(int)
	 */
	public CouchbaseTransactionManager() {}

	/**
	 * Create a new {@link CouchbaseTransactionManager} obtaining sessions from the given {@link CouchbaseClientFactory}
	 * applying the given {@link TransactionOptions options}, if present, when starting a new transaction.
	 *
	 * @param databaseFactory must not be {@literal null}. @//param options can be {@literal null}.
	 */
	public CouchbaseTransactionManager(CouchbaseClientFactory databaseFactory) {

		Assert.notNull(databaseFactory, "DbFactory must not be null!");
		System.err.println(this);
		System.err.println(databaseFactory.getCluster());
		this.databaseFactory = databaseFactory;
	}

	/*
	 * (non-Javadoc)
	 * org.springframework.transaction.support.AbstractPlatformTransactionManager#doGetTransaction()
	 */
	@Override
	protected Object doGetTransaction() throws TransactionException {

		CouchbaseResourceHolder resourceHolder = (CouchbaseResourceHolder) TransactionSynchronizationManager
				.getResource(getRequiredDatabaseFactory().getCluster());
		return new CouchbaseTransactionObject(resourceHolder);
	}

	/*
	 * (non-Javadoc)
	 * org.springframework.transaction.support.AbstractPlatformTransactionManager#isExistingTransaction(java.lang.Object)
	 */
	@Override
	protected boolean isExistingTransaction(Object transaction) throws TransactionException {
		return extractCouchbaseTransaction(transaction).hasResourceHolder();
	}

	/*
	 * (non-Javadoc)
	 * org.springframework.transaction.support.AbstractPlatformTransactionManager#doBegin(java.lang.Object, org.springframework.transaction.TransactionDefinition)
	 */
	@Override
	protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {

		CouchbaseTransactionObject couchbaseTransactionObject = extractCouchbaseTransaction(transaction);
// 	should ACR already be in TSM?	TransactionSynchronizationManager.bindResource(getRequiredDbFactory().getCluster(), resourceHolder);
		CouchbaseResourceHolder resourceHolder = newResourceHolder(definition,
				ClientSessionOptions.builder().causallyConsistent(true).build(),
				null /* ((CouchbaseTransactionDefinition) definition).getAttemptContextReactive()*/);
		couchbaseTransactionObject.setResourceHolder(resourceHolder);

		if (logger.isDebugEnabled()) {
			logger
					.debug(String.format("About to start transaction for session %s.", debugString(resourceHolder.getSession())));
		}

		try {
			couchbaseTransactionObject.startTransaction(options);
		} catch (CouchbaseException ex) {
			throw new TransactionSystemException(String.format("Could not start Mongo transaction for session %s.",
					debugString(couchbaseTransactionObject.getSession())), ex);
		}

		if (logger.isDebugEnabled()) {
			logger.debug(String.format("Started transaction for session %s.", debugString(resourceHolder.getSession())));
		}

		resourceHolder.setSynchronizedWithTransaction(true);
		System.err.println("CouchbaseTransactionManager: "+this);
		System.err.println("bindResource: "+ getRequiredDatabaseFactory().getCluster()+" value: "+resourceHolder);
		TransactionSynchronizationManager.bindResource(getRequiredDatabaseFactory().getCluster(), resourceHolder);
	}

	/*
	 * (non-Javadoc)
	 * org.springframework.transaction.support.AbstractPlatformTransactionManager#doSuspend(java.lang.Object)
	 */
	@Override
	protected Object doSuspend(Object transaction) throws TransactionException {

		CouchbaseTransactionObject couchbaseTransactionObject = extractCouchbaseTransaction(transaction);
		couchbaseTransactionObject.setResourceHolder(null);

		return TransactionSynchronizationManager.unbindResource(getRequiredDatabaseFactory().getCluster());
	}

	/*
	 * (non-Javadoc)
	 * org.springframework.transaction.support.AbstractPlatformTransactionManager#doResume(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected void doResume(@Nullable Object transaction, Object suspendedResources) {
		TransactionSynchronizationManager.bindResource(getRequiredDatabaseFactory().getCluster(), suspendedResources);
	}

	/*
	 * (non-Javadoc)
	 * org.springframework.transaction.support.AbstractPlatformTransactionManager#doCommit(org.springframework.transaction.support.DefaultTransactionStatus)
	 */
	@Override
	protected final void doCommit(DefaultTransactionStatus status) throws TransactionException {

		CouchbaseTransactionObject couchbaseTransactionObject = extractCouchbaseTransaction(status);

		if (logger.isDebugEnabled()) {
			logger.debug(String.format("About to commit transaction for session %s.",
					debugString(couchbaseTransactionObject.getSession())));
		}

		try {
			doCommit(couchbaseTransactionObject);
		} catch (Exception ex) {
			logger.debug("could not commit Couchbase transaction for session "+debugString(couchbaseTransactionObject.getSession()));
			throw new TransactionSystemException(String.format("Could not commit Couchbase transaction for session %s.",
					debugString(couchbaseTransactionObject.getSession())), ex);
		}
	}

	/**
	 * Customization hook to perform an actual commit of the given transaction.<br />
	 * If a commit operation encounters an error, the MongoDB driver throws a {@link CouchbaseException} holding
	 * {@literal error labels}. <br />
	 * By default those labels are ignored, nevertheless one might check for
	 * {@link CouchbaseException#UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL transient commit errors labels} and retry the the
	 * commit. <br />
	 * <code>
	 *     <pre>
	 * int retries = 3;
	 * do {
	 *     try {
	 *         transactionObject.commitTransaction();
	 *         break;
	 *     } catch (CouchbaseException ex) {
	 *         if (!ex.hasErrorLabel(CouchbaseException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
	 *             throw ex;
	 *         }
	 *     }
	 *     Thread.sleep(500);
	 * } while (--retries > 0);
	 *     </pre>
	 * </code>
	 *
	 * @param transactionObject never {@literal null}.
	 * @throws Exception in case of transaction errors.
	 */
	protected void doCommit(CouchbaseTransactionObject transactionObject) throws Exception {
		transactionObject.commitTransaction();
	}

	/*
	 * (non-Javadoc)
	 * org.springframework.transaction.support.AbstractPlatformTransactionManager#doRollback(org.springframework.transaction.support.DefaultTransactionStatus)
	 */
	@Override
	protected void doRollback(DefaultTransactionStatus status) throws TransactionException {

		CouchbaseTransactionObject couchbaseTransactionObject = extractCouchbaseTransaction(status);

		if (logger.isDebugEnabled()) {
			logger.debug(String.format("About to abort transaction for session %s.",
					debugString(couchbaseTransactionObject.getSession())));
		}

		try {
			couchbaseTransactionObject.abortTransaction();
		} catch (CouchbaseException ex) {

			throw new TransactionSystemException(String.format("Could not abort Couchbase transaction for session %s.",
					debugString(couchbaseTransactionObject.getSession())), ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * org.springframework.transaction.support.AbstractPlatformTransactionManager#doSetRollbackOnly(org.springframework.transaction.support.DefaultTransactionStatus)
	 */
	@Override
	protected void doSetRollbackOnly(DefaultTransactionStatus status) throws TransactionException {

		CouchbaseTransactionObject transactionObject = extractCouchbaseTransaction(status);
		transactionObject.getRequiredResourceHolder().setRollbackOnly();
	}

	/*
	 * (non-Javadoc)
	 * org.springframework.transaction.support.AbstractPlatformTransactionManager#doCleanupAfterCompletion(java.lang.Object)
	 */
	@Override
	protected void doCleanupAfterCompletion(Object transaction) {

		Assert.isInstanceOf(CouchbaseTransactionObject.class, transaction,
				() -> String.format("Expected to find a %s but it turned out to be %s.", CouchbaseTransactionObject.class,
						transaction.getClass()));

		CouchbaseTransactionObject couchbaseTransactionObject = (CouchbaseTransactionObject) transaction;

		// Remove the connection holder from the thread.
		TransactionSynchronizationManager.unbindResource(getRequiredDatabaseFactory().getCluster());
		couchbaseTransactionObject.getRequiredResourceHolder().clear();

		if (logger.isDebugEnabled()) {
			logger.debug(String.format("About to release Session %s after transaction.",
					debugString(couchbaseTransactionObject.getSession())));
		}

		couchbaseTransactionObject.closeSession();
	}

	/**
	 * Set the {@link CouchbaseClientFactory} that this instance should manage transactions for.
	 *
	 * @param databaseFactory must not be {@literal null}.
	 */
	public void setDbFactory(CouchbaseClientFactory databaseFactory) {

		Assert.notNull(databaseFactory, "DbFactory must not be null!");
		this.databaseFactory = databaseFactory;
	}

	/**
	 * Set the {@link TransactionOptions} to be applied when starting transactions.
	 *
	 * @param options can be {@literal null}.
	 */
	public void setOptions(@Nullable TransactionOptions options) {
		this.options = options;
	}

	/**
	 * Get the {@link CouchbaseClientFactory} that this instance manages transactions for.
	 *
	 * @return can be {@literal null}.
	 */
	@Nullable
	public CouchbaseClientFactory getDbFactory() {
		return databaseFactory;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.transaction.support.ResourceTransactionManager#getResourceFactory()
	 */
	@Override
	public CouchbaseClientFactory getResourceFactory() {
		return getRequiredDatabaseFactory();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() {
		getRequiredDatabaseFactory();
	}

	private CouchbaseResourceHolder newResourceHolder(TransactionDefinition definition, ClientSessionOptions options,
			ReactiveTransactionAttemptContext atr) {

		CouchbaseClientFactory databaseFactory = getResourceFactory();

		CouchbaseResourceHolder resourceHolder = new CouchbaseResourceHolder(
				databaseFactory.getSession(options, atr), databaseFactory);
		// TODO resourceHolder.setTimeoutIfNotDefaulted(determineTimeout(definition));

		return resourceHolder;
	}

	/**
	 * @throws IllegalStateException if {@link #databaseFactory} is {@literal null}.
	 */
	private CouchbaseClientFactory getRequiredDatabaseFactory() {

		Assert.state(databaseFactory != null,
				"MongoTransactionManager operates upon a MongoDbFactory. Did you forget to provide one? It's required.");

		return databaseFactory;
	}

	private static CouchbaseTransactionObject extractCouchbaseTransaction(Object transaction) {

		Assert.isInstanceOf(CouchbaseTransactionObject.class, transaction,
				() -> String.format("Expected to find a %s but it turned out to be %s.", CouchbaseTransactionObject.class,
						transaction.getClass()));

		return (CouchbaseTransactionObject) transaction;
	}

	private static CouchbaseTransactionObject extractCouchbaseTransaction(DefaultTransactionStatus status) {

		Assert.isInstanceOf(CouchbaseTransactionObject.class, status.getTransaction(),
				() -> String.format("Expected to find a %s but it turned out to be %s.", CouchbaseTransactionObject.class,
						status.getTransaction().getClass()));

		return (CouchbaseTransactionObject) status.getTransaction();
	}

	private static String debugString(@Nullable ClientSession session) {

		if (session == null) {
			return "null";
		}

		String debugString = String.format("[%s@%s ", ClassUtils.getShortName(session.getClass()),
				Integer.toHexString(session.hashCode()));

		try {
			if (session.getServerSession() != null) {
				debugString += String.format("id = %s, ", session.getServerSession().getIdentifier());
				debugString += String.format("causallyConsistent = %s, ", session.isCausallyConsistent());
				debugString += String.format("txActive = %s, ", session.hasActiveTransaction());
				debugString += String.format("txNumber = %d, ", session.getServerSession().getTransactionNumber());
				debugString += String.format("closed = %d, ", session.getServerSession().isClosed());
				debugString += String.format("clusterTime = %s", session.getClusterTime());
			} else {
				debugString += "id = n/a";
				debugString += String.format("causallyConsistent = %s, ", session.isCausallyConsistent());
				debugString += String.format("txActive = %s, ", session.hasActiveTransaction());
				debugString += String.format("clusterTime = %s", session.getClusterTime());
			}
		} catch (RuntimeException e) {
			debugString += String.format("error = %s", e.getMessage());
		}

		debugString += "]";

		return debugString;
	}

	public CouchbaseClientFactory getDatabaseFactory() {
		return databaseFactory;
	}

	/**
	 * MongoDB specific transaction object, representing a {@link CouchbaseResourceHolder}. Used as transaction object by
	 * {@link CouchbaseTransactionManager}.
	 *
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 * @since 2.1
	 * @see CouchbaseResourceHolder
	 */
	protected static class CouchbaseTransactionObject implements SmartTransactionObject {

		private @Nullable CouchbaseResourceHolder resourceHolder;

		CouchbaseTransactionObject(@Nullable CouchbaseResourceHolder resourceHolder) {
			this.resourceHolder = resourceHolder;
		}

		/**
		 * Set the {@link CouchbaseResourceHolder}.
		 *
		 * @param resourceHolder can be {@literal null}.
		 */
		void setResourceHolder(@Nullable CouchbaseResourceHolder resourceHolder) {
			this.resourceHolder = resourceHolder;
		}

		/**
		 * @return {@literal true} if a {@link CouchbaseResourceHolder} is set.
		 */
		final boolean hasResourceHolder() {
			return resourceHolder != null;
		}

		/**
		 * Start a MongoDB transaction optionally given {@link TransactionOptions}.
		 *
		 * @param options can be {@literal null}
		 */
		void startTransaction(TransactionOptions options) {

			ClientSession session = getRequiredSession();
			// if (options != null) {
			// session.startTransaction(options);
			// } else {
			session.startTransaction();
			// }
		}

		/**
		 * Commit the transaction.
		 */
		public void commitTransaction() {
			((Mono<Void>)(getRequiredSession().commitTransaction())).block();
		}

		/**
		 * Rollback (abort) the transaction.
		 */
		public void abortTransaction() {
			((Mono<Void>)(getRequiredSession().abortTransaction())).block();
		}

		/**
		 * Close a {@link ClientSession} without regard to its transactional state.
		 */
		void closeSession() {

			ClientSession session = getRequiredSession();
			if (session.getServerSession() != null && !session.getServerSession().isClosed()) {
				session.close();
			}
		}

		@Nullable
		public ClientSession getSession() {
			return resourceHolder != null ? resourceHolder.getSession() : null;
		}

		private CouchbaseResourceHolder getRequiredResourceHolder() {

			Assert.state(resourceHolder != null, "CouchbaseResourceHolder is required but not present. o_O");
			return resourceHolder;
		}

		private ClientSession getRequiredSession() {

			ClientSession session = getSession();
			Assert.state(session != null, "A Session is required but it turned out to be null.");
			return session;
		}

		/*
		 * (non-Javadoc)
		 * @see  org.springframework.transaction.support.SmartTransactionObject#isRollbackOnly()
		 */
		@Override
		public boolean isRollbackOnly() {
			return this.resourceHolder != null && this.resourceHolder.isRollbackOnly();
		}

		/*
		 * (non-Javadoc)
		 * @see  org.springframework.transaction.support.SmartTransactionObject#flush()
		 */
		@Override
		public void flush() {
			TransactionSynchronizationUtils.triggerFlush();
		}

	}
}
