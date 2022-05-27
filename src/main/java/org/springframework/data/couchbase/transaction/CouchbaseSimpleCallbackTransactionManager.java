/*
 * Copyright 2021 the original author or authors
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
package org.springframework.data.couchbase.transaction;

import com.couchbase.client.java.transactions.AttemptContextReactiveAccessor;
import com.couchbase.client.java.transactions.TransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionResult;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.ReactiveCouchbaseClientFactory;
import org.springframework.lang.Nullable;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.CallbackPreferringPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

public class CouchbaseSimpleCallbackTransactionManager implements CallbackPreferringPlatformTransactionManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseSimpleCallbackTransactionManager.class);

	private final ReactiveCouchbaseClientFactory couchbaseClientFactory;
	private TransactionOptions options;

	public CouchbaseSimpleCallbackTransactionManager(ReactiveCouchbaseClientFactory couchbaseClientFactory, TransactionOptions options) {
		this.couchbaseClientFactory = couchbaseClientFactory;
		this.options = options;
	}

	@Override
	public <T> T execute(TransactionDefinition definition, TransactionCallback<T> callback) throws TransactionException {
		boolean createNewTransaction = handlePropagation(definition);

		setOptionsFromDefinition(definition);

		if (createNewTransaction) {
			return executeNewTransaction(callback);
		}
		else {
			return callback.doInTransaction(null);
		}
	}

	private <T> T executeNewTransaction(TransactionCallback<T> callback) {
		final AtomicReference<T> execResult = new AtomicReference<>();

		TransactionResult result = couchbaseClientFactory.getCluster().transactions().run(ctx -> {
			CouchbaseTransactionStatus status = new CouchbaseTransactionStatus(null, true, false, false, true, null, null);

			populateTransactionSynchronizationManager(ctx);

			try {
				execResult.set(callback.doInTransaction(status));
			} finally {
				TransactionSynchronizationManager.clear();
			}
		}, this.options);

		TransactionSynchronizationManager.clear();

		return execResult.get();
	}

	// Propagation defines what happens when a @Transactional method is called from another @Transactional method.
	private boolean handlePropagation(TransactionDefinition definition) {
		boolean isExistingTransaction = TransactionSynchronizationManager.isActualTransactionActive();

		LOGGER.trace("Deciding propagation behaviour from {} and {}", definition.getPropagationBehavior(), isExistingTransaction);

		switch (definition.getPropagationBehavior()) {
			case TransactionDefinition.PROPAGATION_REQUIRED:
				// Make a new transaction if required, else just execute the new method in the current transaction.
				return !isExistingTransaction;

			case TransactionDefinition.PROPAGATION_SUPPORTS:
				// Don't appear to have the ability to execute the callback non-transactionally in this layer.
				throw new IllegalTransactionStateException(
						"Propagation level 'support' has been specified which is not supported");

			case TransactionDefinition.PROPAGATION_MANDATORY:
				if (!isExistingTransaction) {
					throw new IllegalTransactionStateException(
							"Propagation level 'mandatory' is specified but not in an active transaction");
				}
				return false;

			case TransactionDefinition.PROPAGATION_REQUIRES_NEW:
				// This requires suspension of the active transaction.  This will be possible to support in a future
				// release, if required.
				throw new IllegalTransactionStateException(
						"Propagation level 'requires_new' has been specified which is not currently supported");

			case TransactionDefinition.PROPAGATION_NOT_SUPPORTED:
				// Don't appear to have the ability to execute the callback non-transactionally in this layer.
				throw new IllegalTransactionStateException(
						"Propagation level 'not_supported' has been specified which is not supported");

			case TransactionDefinition.PROPAGATION_NEVER:
				if (isExistingTransaction) {
					throw new IllegalTransactionStateException(
							"Existing transaction found for transaction marked with propagation 'never'");
				}
				return true;

			case TransactionDefinition.PROPAGATION_NESTED:
				if (isExistingTransaction) {
					// Couchbase transactions cannot be nested.
					throw new IllegalTransactionStateException(
							"Propagation level 'nested' has been specified which is not supported");
				}
				return true;

			default:
				throw new IllegalTransactionStateException(
						"Unknown propagation level " + definition.getPropagationBehavior() + " has been specified");
		}
	}

	/**
	 * @param definition reflects the @Transactional options
	 */
	private void setOptionsFromDefinition(TransactionDefinition definition) {
		if (definition != null) {
			if (definition.getTimeout() != TransactionDefinition.TIMEOUT_DEFAULT) {
				options = options.timeout(Duration.ofSeconds(definition.getTimeout()));
			}

			if (!(definition.getIsolationLevel() == TransactionDefinition.ISOLATION_DEFAULT
					|| definition.getIsolationLevel() == TransactionDefinition.ISOLATION_READ_COMMITTED)) {
				throw new IllegalArgumentException("Couchbase Transactions run at Read Committed isolation - other isolation levels are not supported");
			}

			// readonly is ignored as it is documented as being a hint that won't necessarily cause writes to fail
		}

	}

	// Setting ThreadLocal storage
	private void populateTransactionSynchronizationManager(TransactionAttemptContext ctx) {
		TransactionSynchronizationManager.setActualTransactionActive(true);
		TransactionSynchronizationManager.initSynchronization();
		ReactiveCouchbaseResourceHolder resourceHolder = new ReactiveCouchbaseResourceHolder(AttemptContextReactiveAccessor.getCore(ctx));
		TransactionSynchronizationManager.unbindResourceIfPossible(couchbaseClientFactory.getCluster());
		TransactionSynchronizationManager.bindResource(couchbaseClientFactory.getCluster(), resourceHolder);
	}

	/**
	 * Test transaction infrastructure uses this to determine if transaction is active
	 *
	 * @param definition
	 * @return
	 * @throws TransactionException
	 */
	@Override
	public TransactionStatus getTransaction(@Nullable TransactionDefinition definition)
			throws TransactionException {
		TransactionStatus status = new DefaultTransactionStatus(		null, true, true,
				false, true, false);
		return status;
	}

	@Override
	public void commit(TransactionStatus status) throws TransactionException {
		// todo gpx somewhat nervous that commit/rollback/getTransaction are all left empty but things seem to be working
		// anyway... - what are these used for exactly?
		LOGGER.debug("NO-OP: Committing Couchbase Transaction with status {}", status);
	}

	@Override
	public void rollback(TransactionStatus status) throws TransactionException {
		LOGGER.warn("NO-OP: Rolling back Couchbase Transaction with status {}", status);
	}

}
