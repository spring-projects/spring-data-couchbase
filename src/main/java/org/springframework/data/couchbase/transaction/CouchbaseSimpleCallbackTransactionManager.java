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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.transactions.AttemptContextReactiveAccessor;
import com.couchbase.client.java.transactions.TransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionResult;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.ReactiveCouchbaseClientFactory;
import org.springframework.data.couchbase.transaction.error.TransactionRollbackRequestedException;
import org.springframework.lang.Nullable;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.ReactiveTransaction;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.CallbackPreferringPlatformTransactionManager;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class CouchbaseSimpleCallbackTransactionManager implements CallbackPreferringPlatformTransactionManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseSimpleCallbackTransactionManager.class);

	private final ReactiveCouchbaseClientFactory couchbaseClientFactory;
	private @Nullable TransactionOptions options;

	public CouchbaseSimpleCallbackTransactionManager(ReactiveCouchbaseClientFactory couchbaseClientFactory) {
		this(couchbaseClientFactory, null);
	}

	/**
	 * This override is for users manually creating a CouchbaseSimpleCallbackTransactionManager, and allows the
	 * TransactionOptions to be overridden.
	 */
	public CouchbaseSimpleCallbackTransactionManager(ReactiveCouchbaseClientFactory couchbaseClientFactory, @Nullable TransactionOptions options) {
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

	@Stability.Internal
	<T> Flux<T> executeReactive(TransactionDefinition definition, org.springframework.transaction.reactive.TransactionCallback<T> callback) {
		return Flux.defer(() -> {
			boolean createNewTransaction = handlePropagation(definition);

			setOptionsFromDefinition(definition);

			if (createNewTransaction) {
				return executeNewReactiveTransaction(callback);
			} else {
				return Mono.error(new IllegalStateException("Unsupported operation"));
			}
		});
	}

	private <T> T executeNewTransaction(TransactionCallback<T> callback) {
		final AtomicReference<T> execResult = new AtomicReference<>();

		// Each of these transactions will block one thread on the underlying SDK's transactions scheduler.  This
		// scheduler is effectivel unlimited, but this can still potentially lead to high thread usage by the application.  If this is
		// an issue then users need to instead use the standard Couchbase reactive transactions SDK.
		TransactionResult result = couchbaseClientFactory.getCluster().transactions().run(ctx -> {
			CouchbaseTransactionStatus status = new CouchbaseTransactionStatus(ctx, true, false, false, true, null);

			populateTransactionSynchronizationManager(ctx);

			try {
				execResult.set(callback.doInTransaction(status));
			} finally {
				clearTransactionSynchronizationManager();
			}

			if (status.isRollbackOnly()) {
				throw new TransactionRollbackRequestedException("TransactionStatus.isRollbackOnly() is set");
			}
		}, this.options);

		clearTransactionSynchronizationManager();

		return execResult.get();
	}

	private <T> Flux<T> executeNewReactiveTransaction(org.springframework.transaction.reactive.TransactionCallback<T> callback) {
		final List<T> out = new ArrayList<>();

		return couchbaseClientFactory.getCluster().reactive().transactions().run(ctx -> {
			return Mono.defer(() -> {
				ReactiveTransaction status = new ReactiveTransaction() {
					boolean rollbackOnly = false;

					@Override
					public boolean isNewTransaction() {
						return true;
					}

					@Override
					public void setRollbackOnly() {
						this.rollbackOnly = true;
					}

					@Override
					public boolean isRollbackOnly() {
						return rollbackOnly;
					}

					@Override
					public boolean isCompleted() {
						return false;
					}
				};

				return Flux.from(callback.doInTransaction(status))
						.doOnNext(v -> out.add(v))
						.then(Mono.defer(() -> {
							if (status.isRollbackOnly()) {
								return Mono.error(new TransactionRollbackRequestedException("TransactionStatus.isRollbackOnly() is set"));
							}
							return Mono.empty();
						}));
			})
					// This reactive context is what tells Spring operations they're inside a transaction.
					.contextWrite(reactiveContext -> {
						CouchbaseResourceHolder resourceHolder = couchbaseClientFactory.getResources(
								TransactionOptions.transactionOptions(), AttemptContextReactiveAccessor.getCore(ctx));
						return reactiveContext.put(CouchbaseResourceHolder.class, resourceHolder);
					});

		}, this.options)
				.thenMany(Flux.defer(() -> Flux.fromIterable(out)));
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

	// Setting ThreadLocal storage.
	// Note there is reactive-equivalent code in ReactiveTransactionsWrapper to sync with
	@Stability.Internal
	public static void populateTransactionSynchronizationManager(TransactionAttemptContext ctx) {
		TransactionSynchronizationManager.setActualTransactionActive(true);
		TransactionSynchronizationManager.initSynchronization();
		CouchbaseResourceHolder resourceHolder = new CouchbaseResourceHolder(AttemptContextReactiveAccessor.getCore(ctx));
		TransactionSynchronizationManager.unbindResourceIfPossible(CouchbaseResourceHolder.class);
		TransactionSynchronizationManager.bindResource(CouchbaseResourceHolder.class, resourceHolder);
	}

	@Stability.Internal
	public static void clearTransactionSynchronizationManager() {
		TransactionSynchronizationManager.unbindResourceIfPossible(CouchbaseResourceHolder.class);
		TransactionSynchronizationManager.clear();
	}

	@Override
	public TransactionStatus getTransaction(@Nullable TransactionDefinition definition)
			throws TransactionException {
		// All Spring transactional code (currently) does not call the getTransaction, commit or rollback methods if
		// the transaction manager is a CallbackPreferringPlatformTransactionManager.
		// So these methods should only be hit if user is using PlatformTransactionManager directly.  Spring supports this,
		// but due to the lambda-based nature of our transactions, we cannot.
		// todo gp replace a lot of IllegalStateException with UnsupportedOperationException
		throw new IllegalStateException("Direct programmatic use of the Couchbase PlatformTransactionManager is not supported");
	}

	@Override
	public void commit(TransactionStatus ignored) throws TransactionException {
		throw new IllegalStateException("Direct programmatic use of the Couchbase PlatformTransactionManager is not supported");
	}

	@Override
	public void rollback(TransactionStatus ignored) throws TransactionException {
		throw new IllegalStateException("Direct programmatic use of the Couchbase PlatformTransactionManager is not supported");
	}
}
