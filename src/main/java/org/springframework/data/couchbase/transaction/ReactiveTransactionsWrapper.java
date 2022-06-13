package org.springframework.data.couchbase.transaction;

import org.springframework.data.couchbase.CouchbaseClientFactory;
import reactor.core.publisher.Mono;

import java.util.function.Function;

import com.couchbase.client.java.transactions.AttemptContextReactiveAccessor;
import com.couchbase.client.java.transactions.ReactiveTransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionResult;
import com.couchbase.client.java.transactions.config.TransactionOptions;

public class ReactiveTransactionsWrapper {
	CouchbaseClientFactory couchbaseClientFactory;

	public ReactiveTransactionsWrapper(CouchbaseClientFactory couchbaseClientFactory) {
		this.couchbaseClientFactory = couchbaseClientFactory;
	}

	public Mono<TransactionResult> run(Function<ReactiveSpringTransactionAttemptContext, Mono<?>> transactionLogic) {
		return run(transactionLogic, null);
	}

	public Mono<TransactionResult> run(Function<ReactiveSpringTransactionAttemptContext, Mono<?>> transactionLogic,
			TransactionOptions perConfig) {
		Function<ReactiveTransactionAttemptContext, Mono<?>> newTransactionLogic = (ctx) -> {

			return transactionLogic.apply(new ReactiveSpringTransactionAttemptContext(ctx))

					// This reactive context is what tells Spring operations they're inside a transaction.
					.contextWrite(reactiveContext -> {
						CouchbaseResourceHolder resourceHolder = new CouchbaseResourceHolder(
								AttemptContextReactiveAccessor.getCore(ctx));
						return reactiveContext.put(CouchbaseResourceHolder.class, resourceHolder);
					});
		};

		return couchbaseClientFactory.getCluster().reactive().transactions().run(newTransactionLogic,
				perConfig);

	}

}
