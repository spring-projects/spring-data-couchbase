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
import com.couchbase.client.java.transactions.TransactionResult;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import com.couchbase.client.java.transactions.error.TransactionCommitAmbiguousException;
import com.couchbase.client.java.transactions.error.TransactionFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.TransactionalSupport;
import org.springframework.data.couchbase.transaction.error.TransactionRollbackRequestedException;
import org.springframework.data.couchbase.transaction.error.TransactionSystemAmbiguousException;
import org.springframework.data.couchbase.transaction.error.TransactionSystemUnambiguousException;
import org.springframework.lang.Nullable;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.ReactiveTransaction;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.CallbackPreferringPlatformTransactionManager;
import org.springframework.transaction.support.TransactionCallback;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The Couchbase transaction manager, providing support for @Transactional methods.
 */
public class CouchbaseCallbackTransactionManager implements CallbackPreferringPlatformTransactionManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseCallbackTransactionManager.class);

	private final CouchbaseClientFactory couchbaseClientFactory;
	private @Nullable TransactionOptions options;

	public CouchbaseCallbackTransactionManager(CouchbaseClientFactory couchbaseClientFactory) {
		this(couchbaseClientFactory, null);
	}

	/**
	 * This override is for users manually creating a CouchbaseCallbackTransactionManager, and allows the
	 * TransactionOptions to be overridden.
	 */
	public CouchbaseCallbackTransactionManager(CouchbaseClientFactory couchbaseClientFactory, @Nullable TransactionOptions options) {
		this.couchbaseClientFactory = couchbaseClientFactory;
		this.options = options != null ? options : TransactionOptions.transactionOptions();
	}

	@Override
	public <T> T execute(TransactionDefinition definition, TransactionCallback<T> callback) throws TransactionException {
		boolean createNewTransaction = handlePropagation(definition);

		setOptionsFromDefinition(definition);

		if (createNewTransaction) {
			return executeNewTransaction(callback);
		} else {
			return callback.doInTransaction(null);
		}
	}

	@Stability.Internal
	<T> Flux<T> executeReactive(TransactionDefinition definition,
			org.springframework.transaction.reactive.TransactionCallback<T> callback) {
		return Flux.defer(() -> {
			boolean createNewTransaction = handlePropagation(definition);

			setOptionsFromDefinition(definition);

			if (createNewTransaction) {
				return executeNewReactiveTransaction(callback);
			} else {
				return Mono.error(new UnsupportedOperationException("Unsupported operation"));
			}
		});
	}

	private <T> T executeNewTransaction(TransactionCallback<T> callback) {
		final AtomicReference<T> execResult = new AtomicReference<>();

		// Each of these transactions will block one thread on the underlying SDK's transactions scheduler. This
		// scheduler is effectively unlimited, but this can still potentially lead to high thread usage by the application.
		// If this is an issue then users need to instead use the standard Couchbase reactive transactions SDK.
		try {
			TransactionResult ignored = couchbaseClientFactory.getCluster().transactions().run(ctx -> {
				CouchbaseTransactionStatus status = new CouchbaseTransactionStatus(ctx, true, false, false, true, null);

				T res = callback.doInTransaction(status);
				if (res instanceof Mono || res instanceof Flux) {
					throw new UnsupportedOperationException("Return type is Mono or Flux, indicating a reactive transaction is being performed in a blocking way.  A potential cause is the CouchbaseTransactionInterceptor is not in use.");
				}
				execResult.set(res);

				if (status.isRollbackOnly()) {
					throw new TransactionRollbackRequestedException("TransactionStatus.isRollbackOnly() is set");
				}
			}, this.options);

			return execResult.get();
		}
		catch (RuntimeException ex) {
			throw convert(ex);
		}
	}

	private static RuntimeException convert(RuntimeException ex) {
		if (ex instanceof TransactionCommitAmbiguousException) {
			return new TransactionSystemAmbiguousException((TransactionCommitAmbiguousException) ex);
		}
		if (ex instanceof TransactionFailedException) {
			return new TransactionSystemUnambiguousException((TransactionFailedException) ex);
		}
		// Should not get here
		return ex;
	}

	private <T> Flux<T> executeNewReactiveTransaction(org.springframework.transaction.reactive.TransactionCallback<T> callback) {
		// Buffer the output rather than attempting to stream results back from a now-defunct lambda.
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
								return Mono
										.error(new TransactionRollbackRequestedException("TransactionStatus.isRollbackOnly() is set"));
							}
							return Mono.empty();
						}));
			});

		}, this.options)
				.thenMany(Flux.defer(() -> Flux.fromIterable(out)))
				.onErrorMap(ex -> {
					if (ex instanceof RuntimeException) {
						return convert((RuntimeException) ex);
					}
					return ex;
				});
	}

	// Propagation defines what happens when a @Transactional method is called from another @Transactional method.
	private boolean handlePropagation(TransactionDefinition definition) {
		boolean isExistingTransaction = TransactionalSupport.checkForTransactionInThreadLocalStorage().block().isPresent();

		LOGGER.trace("Deciding propagation behaviour from {} and {}", definition.getPropagationBehavior(),
				isExistingTransaction);

		switch (definition.getPropagationBehavior()) {
			case TransactionDefinition.PROPAGATION_REQUIRED:
				// Make a new transaction if required, else just execute the new method in the current transaction.
				return !isExistingTransaction;

			case TransactionDefinition.PROPAGATION_SUPPORTS:
				// Don't appear to have the ability to execute the callback non-transactionally in this layer.
				throw new UnsupportedOperationException(
						"Propagation level 'support' has been specified which is not supported");

			case TransactionDefinition.PROPAGATION_MANDATORY:
				if (!isExistingTransaction) {
					throw new IllegalTransactionStateException(
							"Propagation level 'mandatory' is specified but not in an active transaction");
				}
				return false;

			case TransactionDefinition.PROPAGATION_REQUIRES_NEW:
				// This requires suspension of the active transaction. This will be possible to support in a future
				// release, if required.
				throw new UnsupportedOperationException(
						"Propagation level 'requires_new' has been specified which is not currently supported");

			case TransactionDefinition.PROPAGATION_NOT_SUPPORTED:
				// Don't appear to have the ability to execute the callback non-transactionally in this layer.
				throw new UnsupportedOperationException(
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
					throw new UnsupportedOperationException(
							"Propagation level 'nested' has been specified which is not supported");
				}
				return true;

			default:
				throw new UnsupportedOperationException(
						"Unknown propagation level " + definition.getPropagationBehavior() + " has been specified");
		}
	}

	/**
	 * @param definition reflects the @Transactional options
	 */
	private void setOptionsFromDefinition(TransactionDefinition definition) {
		if (definition != null) {
			if (definition.getTimeout() != TransactionDefinition.TIMEOUT_DEFAULT) {
				if (options == null) {
					options = TransactionOptions.transactionOptions();
				}
				options = options.timeout(Duration.ofSeconds(definition.getTimeout()));
			}

			if (!(definition.getIsolationLevel() == TransactionDefinition.ISOLATION_DEFAULT
					|| definition.getIsolationLevel() == TransactionDefinition.ISOLATION_READ_COMMITTED)) {
				throw new IllegalArgumentException(
						"Couchbase Transactions run at Read Committed isolation - other isolation levels are not supported");
			}

			// readonly is ignored as it is documented as being a hint that won't necessarily cause writes to fail
		}

	}

	@Override
	public TransactionStatus getTransaction(@Nullable TransactionDefinition definition) throws TransactionException {
		// All Spring transactional code (currently) does not call the getTransaction, commit or rollback methods if
		// the transaction manager is a CallbackPreferringPlatformTransactionManager.
		// So these methods should only be hit if user is using PlatformTransactionManager directly. Spring supports this,
		// but due to the lambda-based nature of our transactions, we cannot.
		throw new UnsupportedOperationException("Direct programmatic use of the Couchbase PlatformTransactionManager is not supported");
	}

	@Override
	public void commit(TransactionStatus ignored) throws TransactionException {
		throw new UnsupportedOperationException("Direct programmatic use of the Couchbase PlatformTransactionManager is not supported");
	}

	@Override
	public void rollback(TransactionStatus ignored) throws TransactionException {
		throw new UnsupportedOperationException("Direct programmatic use of the Couchbase PlatformTransactionManager is not supported");
	}
}
