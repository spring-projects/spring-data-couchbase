package org.springframework.data.couchbase.transaction;

import reactor.core.publisher.Mono;

import java.util.function.Function;

import org.springframework.data.couchbase.ReactiveCouchbaseClientFactory;
import org.springframework.transaction.ReactiveTransaction;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.reactive.TransactionContextManager;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;

import com.couchbase.client.java.transactions.AttemptContextReactiveAccessor;
import com.couchbase.client.java.transactions.ReactiveTransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionResult;
import com.couchbase.client.java.transactions.config.TransactionOptions;

// todo gp needed now Transactions has gone?
public class ReactiveTransactionsWrapper /* wraps ReactiveTransactions */ {
	ReactiveCouchbaseClientFactory reactiveCouchbaseClientFactory;

	public ReactiveTransactionsWrapper(ReactiveCouchbaseClientFactory reactiveCouchbaseClientFactory) {
		this.reactiveCouchbaseClientFactory = reactiveCouchbaseClientFactory;
	}

	/**
	 * A convenience wrapper around {@link TransactionsReactive#run}, that provides a default
	 * <code>PerTransactionConfig</code>.
	 */

	public Mono<TransactionResult> run(Function<ReactiveTransactionAttemptContext, Mono<?>> transactionLogic) {
		return run(transactionLogic, null);
	}

	public Mono<TransactionResult> run(Function<ReactiveTransactionAttemptContext, Mono<?>> transactionLogic,
			TransactionOptions perConfig) {
		// todo gp this is duplicating a lot of logic from the core loop, and is hopefully not needed.
		// todo ^^^ I think I removed all the duplicate logic.
		Function<ReactiveTransactionAttemptContext, Mono<?>> newTransactionLogic = (ctx) -> {
			ReactiveCouchbaseResourceHolder resourceHolder = reactiveCouchbaseClientFactory.getResources(
					TransactionOptions.transactionOptions(), AttemptContextReactiveAccessor.getCore(ctx));
			Mono<TransactionSynchronizationManager> sync = TransactionContextManager.currentContext()
					.map(TransactionSynchronizationManager::new).flatMap(synchronizationManager -> {
						synchronizationManager.bindResource(reactiveCouchbaseClientFactory.getCluster(), resourceHolder);
						prepareSynchronization(synchronizationManager, null, new CouchbaseTransactionDefinition());
						return transactionLogic.apply(ctx) // <---- execute the transaction
								.thenReturn(ctx).then(Mono.just(synchronizationManager));
					});
			return sync.contextWrite(TransactionContextManager.getOrCreateContext())
					.contextWrite(TransactionContextManager.getOrCreateContextHolder());
		};

		return reactiveCouchbaseClientFactory.getCluster().reactive().transactions().run(newTransactionLogic,
				perConfig);

	}

	private static void prepareSynchronization(TransactionSynchronizationManager synchronizationManager,
			ReactiveTransaction status, TransactionDefinition definition) {
		// if (status.isNewTransaction()) {
		synchronizationManager.setActualTransactionActive(false /*status.hasTransaction()*/);
		synchronizationManager.setCurrentTransactionIsolationLevel(
				definition.getIsolationLevel() != TransactionDefinition.ISOLATION_DEFAULT ? definition.getIsolationLevel()
						: null);
		synchronizationManager.setCurrentTransactionReadOnly(definition.isReadOnly());
		synchronizationManager.setCurrentTransactionName(definition.getName());
		synchronizationManager.initSynchronization();
		// }
	}

}
