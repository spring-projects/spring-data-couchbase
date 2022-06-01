package org.springframework.data.couchbase.transaction;

import reactor.core.publisher.Mono;

import java.util.function.Function;

import org.springframework.data.couchbase.ReactiveCouchbaseClientFactory;

import com.couchbase.client.java.transactions.AttemptContextReactiveAccessor;
import com.couchbase.client.java.transactions.ReactiveTransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionResult;
import com.couchbase.client.java.transactions.config.TransactionOptions;

public class ReactiveTransactionsWrapper {
	ReactiveCouchbaseClientFactory reactiveCouchbaseClientFactory;

	public ReactiveTransactionsWrapper(ReactiveCouchbaseClientFactory reactiveCouchbaseClientFactory) {
		this.reactiveCouchbaseClientFactory = reactiveCouchbaseClientFactory;
	}

	public Mono<TransactionResult> run(Function<ReactiveTransactionAttemptContext, Mono<?>> transactionLogic) {
		return run(transactionLogic, null);
	}

	// todo gp maybe instead of giving them a ReactiveTransactionAttemptContext we give them a wrapped version, in case we ever need Spring-specific functionality
	public Mono<TransactionResult> run(Function<ReactiveTransactionAttemptContext, Mono<?>> transactionLogic,
			TransactionOptions perConfig) {
		Function<ReactiveTransactionAttemptContext, Mono<?>> newTransactionLogic = (ctx) -> {

			return transactionLogic.apply(ctx)

					// This reactive context is what tells Spring operations they're inside a transaction.
					.contextWrite(reactiveContext -> {
						ReactiveCouchbaseResourceHolder resourceHolder = reactiveCouchbaseClientFactory.getResources(
								TransactionOptions.transactionOptions(), AttemptContextReactiveAccessor.getCore(ctx));
						return reactiveContext.put(ReactiveCouchbaseResourceHolder.class, resourceHolder);
					});
		};

		return reactiveCouchbaseClientFactory.getCluster().reactive().transactions().run(newTransactionLogic,
				perConfig);

	}
}
