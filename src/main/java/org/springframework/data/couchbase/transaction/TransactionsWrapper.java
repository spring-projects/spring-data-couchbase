package org.springframework.data.couchbase.transaction;

import org.springframework.data.couchbase.core.TransactionalSupport;
import reactor.util.annotation.Nullable;

import java.util.function.Consumer;

import org.springframework.data.couchbase.CouchbaseClientFactory;

import com.couchbase.client.core.error.transaction.internal.CoreTransactionFailedException;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.core.transaction.CoreTransactionResult;
import com.couchbase.client.core.transaction.log.CoreTransactionLogger;
import com.couchbase.client.java.transactions.AttemptContextReactiveAccessor;
import com.couchbase.client.java.transactions.TransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionResult;
import com.couchbase.client.java.transactions.Transactions;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import com.couchbase.client.java.transactions.error.TransactionFailedException;

public class TransactionsWrapper {
	CouchbaseClientFactory couchbaseClientFactory;

	public TransactionsWrapper(CouchbaseClientFactory couchbaseClientFactory) {
		this.couchbaseClientFactory = couchbaseClientFactory;
	}

	/**
	 * Runs supplied transactional logic until success or failure.
	 * <p>
	 * The supplied transactional logic will be run if necessary multiple times, until either:
	 * <ul>
	 * <li>The transaction successfully commits</li>
	 * <li>The transactional logic requests an explicit rollback</li>
	 * <li>The transaction timesout.</li>
	 * <li>An exception is thrown, either inside the transaction library or by the supplied transaction logic, that cannot
	 * be handled.
	 * </ul>
	 * <p>
	 * The transaction logic {@link Consumer} is provided an {@link TransactionAttemptContext}, which contains methods
	 * allowing it to read, mutate, insert and delete documents, as well as commit or rollback the transaction.
	 * <p>
	 * If the transaction logic performs a commit or rollback it must be the last operation performed. Else a
	 * {@link com.couchbase.client.java.transactions.error.TransactionFailedException} will be thrown. Similarly, there
	 * cannot be a commit followed by a rollback, or vice versa - this will also raise a
	 * {@link CoreTransactionFailedException}.
	 * <p>
	 * If the transaction logic does not perform an explicit commit or rollback, then a commit will be performed anyway.
	 *
	 * @param transactionLogic the application's transaction logic
	 * @param options the configuration to use for this transaction
	 * @return there is no need to check the returned {@link CoreTransactionResult}, as success is implied by the lack of
	 *         a thrown exception. It contains information useful only for debugging and logging.
	 * @throws TransactionFailedException or a derived exception if the transaction fails to commit for any reason,
	 *           possibly after multiple retries. The exception contains further details of the error
	 */

	public TransactionResult run(Consumer<SpringTransactionAttemptContext> transactionLogic,
			@Nullable TransactionOptions options) {
		Consumer<TransactionAttemptContext> newTransactionLogic = (ctx) -> {
			try {
				CoreTransactionLogger logger = AttemptContextReactiveAccessor.getLogger(ctx);
				CoreTransactionAttemptContext atr = AttemptContextReactiveAccessor.getCore(ctx);

				// from CouchbaseTransactionManager
				CouchbaseResourceHolder resourceHolder = TransactionalSupport.newResourceHolder(couchbaseClientFactory,
						/*definition*/ new CouchbaseTransactionDefinition(), TransactionOptions.transactionOptions(), atr);
				// couchbaseTransactionObject.setResourceHolder(resourceHolder);

				logger
						.debug(String.format("About to start transaction for session %s.", TransactionalSupport.debugString(resourceHolder.getCore())));

				logger.debug(String.format("Started transaction for session %s.", TransactionalSupport.debugString(resourceHolder.getCore())));

				CouchbaseSimpleCallbackTransactionManager.populateTransactionSynchronizationManager(ctx);

				transactionLogic.accept(new SpringTransactionAttemptContext(ctx));
			} finally {
				CouchbaseSimpleCallbackTransactionManager.clearTransactionSynchronizationManager();
			}
		};

		return AttemptContextReactiveAccessor.run(couchbaseClientFactory.getCluster().transactions(), newTransactionLogic,
				options == null ? null : options.build());
	}

	/**
	 * Runs supplied transactional logic until success or failure. A convenience overload for {@link Transactions#run}
	 * that provides a default <code>PerTransactionConfig</code>
	 */

	public TransactionResult run(Consumer<SpringTransactionAttemptContext> transactionLogic) {
		return run(transactionLogic, null);
	}

}
